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
# and grew further in #3229 (the `(cx*)` docs-census family). It is a single
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
# THE SAME PATH, NEVER REASSIGNED (#3367). `WRAPPER` is deliberately repointed at a scratch
# copy by the gate-mock cases (~2251/~2354) and restored after, so ANY check that READS the
# wrapper as a doctrine SUBJECT is order-dependent if it reads `WRAPPER`: its verdict then
# depends on a global set ~2000 lines away, which is what made the classifier-GONE assert
# flip between runs at an unchanged tree. Structural scans read THIS variable; only the cases that
# EXECUTE the wrapper in their own context keep using `WRAPPER`. NOTE that COPY sites do NOT: every
# `cp` of the wrapper was moved to `WRAPPER_REAL`, and the grammar below has no permitted form for a
# copy, so following an older instruction to "keep using WRAPPER for copies" now reds the gate.
#
# Note the direction of the hazard now: with the `--help` prose exemption in place, a scan
# that lands on the scratch copy finds nothing and reports CLEAN — a false pass, which is
# worse than the false fail that surfaced the bug.
unset WRAPPER 2>/dev/null || true
readonly WRAPPER_REAL="$SCRIPT_DIR/../flow/roborev-review.sh"
# THE SCRATCH-COPY OVERRIDE STARTS EMPTY, WHATEVER THE CALLER EXPORTED (roborev job 53). It is read
# as `${RUN_WRAPPER_PATH:-$WRAPPER_REAL}`, so an inherited environment value would silently redirect
# every early run_wrapper call at an arbitrary script and the suite would report on that instead --
# a hermeticity hole introduced by this very refactor. Only the two gate-mock cases may set it.
# THE MUTABLE GLOBAL IS FORBIDDEN BY THE SHELL, NOT BY A GREP (roborev job 54). `readonly` on an
# UNSET name refuses every later assignment form -- `WRAPPER=`, `export WRAPPER=`, `declare
# WRAPPER=`, `WRAPPER+=`, `printf -v WRAPPER`, `read WRAPPER` (all six measured). The structural
# check below matched only a bare `WRAPPER=`, which is the assignment-syntax enumeration roborev
# already corrected me on for WRAPPER_REAL in job 37; I repeated it on the new check. Nothing reads
# $WRAPPER any more, so leaving it unset costs nothing and makes reintroduction impossible rather
# than merely detected. UNSET FIRST: `readonly` on an INHERITED exported value freezes that value
# rather than forbidding one (measured: `WRAPPER=/decoy bash -c 'readonly WRAPPER; echo $WRAPPER'`
# prints /decoy), which would make the suite red on the caller environment (roborev job 56).
readonly WRAPPER
# The structured waiver scanner the wrapper delegates to (#3312 job 26). Defined HERE, beside
# WRAPPER, because run_wrapper passes it on every call — the structural section is too late.
SCAN_TOOL="$SCRIPT_DIR/../flow/roborev-waiver-scan.py"
TEST_SELF="$SCRIPT_DIR/$(basename "$0")"
# The two sourced halves, named ONCE here because several cases probe a function DIRECTLY rather
# than through the wrapper. `WRAPPER` is reassigned later by the gate-mock cases; these are not.
CHECKS_SRC="$SCRIPT_DIR/../flow/roborev-review-checks.sh"
ORACLES_SRC="$SCRIPT_DIR/../flow/roborev-review-oracles.sh"
BLOCK_HEADER="==== ROBOREV REVIEW SUMMARY ===="

if [ ! -f "$WRAPPER_REAL" ]; then
  printf 'FAIL - wrapper not found at %s\n' "$WRAPPER_REAL"
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

# mktemp is verified BEFORE the trap and before any fixture (#3296). Unchecked, a failed or
# empty-output `mktemp -d` leaves $tmp empty, every `"$tmp/x"` resolves to `/x`, and this file
# creates those paths unconditionally — root-level file creation on a privileged runner. Arming
# `rm -rf "$tmp"` on an unverified path is the same hazard, hence the ordering. Non-empty AND a
# directory: a diagnostic printed on stdout would pass the emptiness test alone.
tmp=$(mktemp -d "${TMPDIR:-/tmp}/roborev-guard-test.XXXXXX") || tmp=''
if [ -z "$tmp" ] || [ ! -d "$tmp" ]; then
  printf 'FAIL - mktemp -d did not yield a usable temp directory (got: %s) — refusing to run rather than resolving every "$tmp/..." fixture path under /\n' "${tmp:-<empty>}"
  exit 1
fi
trap 'rm -rf "$tmp"' EXIT

# ---------------------------------------------------------------------------
# PORTABILITY (#3296): the ONE in-place edit used by every case that mutates a copied
# flow script. `sed -i EXPR FILE` is GNU-ONLY, and its BSD failure is SILENT-ish:
# BSD/macOS sed declares -i with a REQUIRED argument (Apple text_cmds sed/main.c:
# `getopt(argc, argv, "EI:ae:f:i:lnru")` — note the `i:`), so it consumes EXPR as the
# backup SUFFIX and then reads FILE as the SCRIPT. The edit never lands. That is how
# four cases (cx28, cx29, cx28b, cx28c) FAILed on macOS at pristine origin/main while
# passing on Linux — each verifies the mutation landed before asserting against it, so
# each correctly reported `bad`: the fail-closed design worked, only the portability
# was wrong.
#
# This helper keeps that fail-closed property and adds a SECOND, independent guard at
# the edit site: it returns NON-ZERO when the file content did not change, so an
# expression that matches nothing is an error here as well as at each case's own
# verification. Portability is therefore not a licence to become unconditional — a
# no-op patch is still detected in both places.
#
# Replacement newlines are written as a backslash + a REAL newline (POSIX §sed, valid
# on every sed) rather than `\n`: current BSD sed does translate `\n` in the RHS
# (text_cmds sed/compile.c compile_subst: `case 'n': *p = '\n';`), but the POSIX form
# is vintage-insensitive and costs nothing.
#
# Do NOT reintroduce `sed -i` (or any other GNU-only construct) here: the structural
# assert in scripts/tests/test_roborev_guard_portability.sh forbids it, and that test
# also exercises this helper under a BSD-semantics `sed` shim.
#
# THE ORIGINAL FILE IS TRUNCATED AND REWRITTEN, NOT REPLACED (#3296 round-6): the first
# form ended in `mv "$_t" "$_f"`, and the scratch file was created fresh by `>`, so it
# carried `0666 & ~umask` — i.e. NO execute bits under ANY umask. Every mutation of an
# executable script (the cases here copy and patch `roborev-review-checks.sh`, mode 755)
# therefore silently made it non-executable: measured `-rwxr-xr-x` -> `-rw-rw-r--`. That
# is the same class as the defect this branch exists to fix — an environment-dependent
# breakage introduced by a portability fix.
#
# Writing the transformed bytes back into the ORIGINAL path with `>` avoids the whole
# question rather than answering it: POSIX redirection opens an EXISTING file with
# O_TRUNC, and the `mode` argument of open() applies only on CREATION, so the file's
# permission bits, owner and inode are untouched. No mode is ever queried — deliberately:
# `stat`'s format flags are themselves GNU-vs-BSD divergent (`stat -c %a` vs `stat -f %Lp`),
# and `cp`'s no-`-p` destination mode is umask-modified, so both of the obvious repairs
# would have reintroduced exactly the platform divergence under test. The scratch file is
# still used as the staging buffer (sed cannot read and write one file at once) and the
# no-change check still runs BEFORE anything is written back, so the fail-closed contract
# is unchanged: a no-op patch touches nothing and returns non-zero.
# ===========================================================================
# CALLER CONTRACT — READ THIS BEFORE ADDING A CALL (#3296).
#
# EVERY caller MUST check the status of this helper. It returns NON-ZERO when the edit did
# not land (sed failed, or the expression matched nothing and the file is unchanged), and
# that return is the ONLY thing standing between a silently unapplied mutation and a case
# that then asserts against STALE content — a pass for a reason the case is not about. That
# is not hypothetical: cx29's restore discarded the status once, and with its expression made
# non-matching the case still reported `ok` on the PREVIOUS case's mutation.
#
# So: `if sed_inplace … ; then` / `elif ! sed_inplace_verified … ; then`, or
# `sed_inplace … || bad "…"`. NEVER a bare statement call, and never a `|| true`.
#
# PREFER `sed_inplace_verified` (below) for any mutate-then-assert case: it additionally
# requires the intended post-edit STATE to be present and, optionally, the state it replaces
# to be gone, so "the edit ran" cannot be mistaken for "the edit did what I meant".
#
# No lint enforces this — the structural rule that tried was DELETED by owner ruling; see the
# "DELIBERATELY NOT BUILT" record in scripts/tests/test_roborev_guard_portability.sh for why,
# and for the residual it leaves. This comment is the enforcement.
# ===========================================================================
sed_inplace() { # sed_inplace <file> <sed-expr>  -> non-zero if nothing changed; CHECK THE STATUS
  local _f="$1" _expr="$2" _t
  _t="$_f.sed-inplace.$$"
  sed "$_expr" "$_f" >"$_t" || { rm -f "$_t"; return 1; }
  if cmp -s "$_f" "$_t"; then rm -f "$_t"; return 1; fi
  cat "$_t" >"$_f" || { rm -f "$_t"; return 1; }
  rm -f "$_t"
}

# AFFIRMATIVE MEASUREMENT AT THE EDIT SITE (#3296, CLAUDE.md "a positive verdict requires an
# AFFIRMATIVE MEASUREMENT"). `sed_inplace`'s non-zero status is only protection if a CALLER
# READS IT, and a mutate-then-assert case that ignores it inherits the permissive branch for
# every unmeasured state: sed matched nothing, sed matched a DIFFERENT line, an earlier case's
# mutation is still in the file. The later assertion is then satisfied by the STALE content the
# edit never replaced — a pass for a reason the case is not about. (Concretely: the cx29 restore
# below ignored the status, and with the expression made non-matching cx29's own causal assert
# `assert_verdict FAIL 1` still read `ok`, on cx28's grammar violation.)
#
# So every mutation in this file goes through this wrapper, which requires THREE affirmative
# facts before the caller may believe its mutation: the edit changed the file, the intended
# post-edit STATE is present, and (optionally) the state it was supposed to replace is GONE.
# Anything else is non-zero, and every call site routes non-zero to `bad`. Fixed-string
# (`grep -F`) comparisons throughout: the states are literal source lines, not patterns.
sed_inplace_verified() { # sed_inplace_verified <file> <expr> <must-be-present> [<must-be-absent>]
  local _f="$1" _expr="$2" _want="$3" _gone="${4:-}"
  sed_inplace "$_f" "$_expr" || return 1
  grep -qF -- "$_want" "$_f" || return 1
  if [ -n "$_gone" ] && grep -qF -- "$_gone" "$_f"; then return 1; fi
  return 0
}

# PORTABILITY (#3296): the summary block's key ORDER, extracted with one awk.
# The previous form was `grep -nE '^(k1|k2|k3):' "$OUT" | cut -d: -f2 | paste -sd,`,
# whose `paste -sd,` carries NO FILE OPERAND. GNU paste reads stdin in that case; BSD
# paste does NOT — Apple text_cmds paste/paste.c and FreeBSD usr.bin/paste/paste.c both
# do `argc -= optind; argv += optind; if (*argv == NULL) usage();`, and `usage()` prints
# "usage: paste [-s] [-d delimiters] file ..." to stderr and exit(1). So on macOS the
# whole command substitution yielded the EMPTY STRING — exactly the reported case (j2)
# symptom: an empty extraction while the two neighbouring asserts on the same $OUT were
# green. `cut` is NOT implicated: FreeBSD usr.bin/cut/cut.c reads stdin when given no
# file operand (`if (*argv) ... else rval = fcn(stdin, "stdin")`).
# awk needs no join step, so the operand-less pipe stage disappears entirely.
summary_key_order() { # summary_key_order <file> <extended-regex-of-keys> -> "k1,k2,k3"
  awk -v keys="$2" '$0 ~ "^(" keys "):" { printf "%s%s", sep, substr($0, 1, index($0, ":") - 1); sep = "," }' "$1"
}

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
  # THE JOB'S OWN BRANCH (#3654 round 2). Real payloads carry it — `.job.branch` on `show`, top
  # level on `list` — and the supplementary machine lookup is scoped BY IT rather than by the
  # branch the invocation happens to be on. `none` suppresses it, which is how the
  # "record does not name its branch" state is fixtured.
  if [ -n "${STUB_JOB_BRANCH:-}" ] && [ "${STUB_JOB_BRANCH:-}" != none ]; then
    extra="$extra,\"branch\":\"${STUB_JOB_BRANCH}\""
  fi
  local git_ref="${STUB_GIT_REF:-${STUB_ANNOUNCE_SHA:-}}"
  if [ "${STUB_GIT_REF:-}" = none ]; then git_ref=""; fi
  if [ -n "${STUB_VERDICT_FIELD:-}" ]; then
    extra="$extra,\"verdict\":\"${STUB_VERDICT_FIELD}\""
  fi
  # ===== `source_machine_id` IS CARRIED BY `list` ROWS ONLY (#3654) =====
  # MEASURED on roborev v0.61.2: `roborev list --json` rows carry it at top level, and
  # `roborev show <id> --json` does not carry it ANYWHERE — not at top level, not in its nested
  # `job` object. The stub reproduces that asymmetry rather than emitting the field everywhere,
  # because it is exactly what makes `NOT RECORDED` a REAL state and what forces the wrapper's
  # supplementary `list` read: a stub that answered on `show` too would make the `list` path
  # untested and the key look correct while reporting nothing on a real run.
  if [ -n "${STUB_SOURCE_MACHINE_ID:-}" ] && [ "${_EMIT_MACHINE:-0}" = 1 ]; then
    extra="$extra,\"source_machine_id\":\"${STUB_SOURCE_MACHINE_ID}\""
  fi
  # The review TEXT the record carries (#3312 job 24). On the JOB row as well as the review row, so every
  # payload shape a recheck may read from carries it, exactly as roborev's own payloads do.
  if [ -n "${STUB_RECORD_OUTPUT:-}" ]; then
    extra="$extra,\"${STUB_RECORD_OUTPUT_FIELD:-output}\":\"${STUB_RECORD_OUTPUT}\""
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
    # STUB_ON_REVIEW: a command run AT ENQUEUE TIME, so a case can mutate the fixture DURING the
    # review — the only way to express a mid-run base-ref move hermetically (#3392). It FAILS LOUDLY
    # (exit 97, distinct from every verdict path) rather than being swallowed: a hook that silently
    # did nothing would leave its case asserting a race it never created, which is a probe failing
    # toward success.
    if [ -n "${STUB_ON_REVIEW:-}" ]; then
      sh -c "$STUB_ON_REVIEW" >/dev/null 2>&1 || {
        printf 'STUB_ON_REVIEW failed: %s\n' "$STUB_ON_REVIEW" >&2
        exit 97
      }
    fi
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
    # STUB_RECORD_OUTPUT is the review TEXT the record carries — what a recheck re-asserts
    # `review-completed`, the vacuity tiers and `findings` against (#3312 job 24). Empty means the record
    # has none, which a recheck must read as "not re-establishable" rather than inherit.
    if [ "${STUB_SHOW_JSON:-object}" = nested ]; then
      # The MEASURED `roborev show <id> --json` shape: a REVIEW row carrying its own
      # `id` (equal to the job id) that NESTS the job row under a "job" key.
      # `verdict_bool` is the SOURCE COLUMN the `verdict` letter is synthesised FROM, so the two
      # must AGREE or the fixture is a record roborev cannot emit: `P` <=> 1, `F` <=> 0 (measured —
      # job 154 clean/verdict_bool=1, job 162 findings-bearing/verdict_bool=0). Hard-coding 0 while
      # a case set STUB_VERDICT_FIELD=P made the CLEAN-recheck positive control an impossible
      # payload, i.e. weakened the one control that proves a clean recheck still PASSes. Derived,
      # never hard-coded, so a case cannot silently desynchronise the pair again.
      stub_verdict_bool=0
      [ "${STUB_VERDICT_FIELD:-}" != P ] || stub_verdict_bool=1
      printf '{"id":%s,"job_id":%s,"agent":"codex","verdict_bool":%s,"%s":"%s","prompt":"%s","job":' \
        "${STUB_JOB:-4600}" "${STUB_JOB:-4600}" "$stub_verdict_bool" "${STUB_RECORD_OUTPUT_FIELD:-output}" "${STUB_RECORD_OUTPUT:-}" "$(json_prompt)"
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
    # ===== `roborev list` IS BRANCH-FILTERED, AND THE STUB HONOURS THAT (#3654 round 2) =====
    # Measured: a bare list from a branch with no jobs returns `null` while `--branch <other>`
    # returns that branch's rows. Modelling it is what makes the branch-scoping cases REAL — a stub
    # that answered whatever branch it was asked would pass both before and after the fix, i.e. the
    # cases would test nothing. STUB_LIST_BRANCH is the branch this daemon has jobs for.
    if [ -n "${STUB_LIST_BRANCH:-}" ]; then
      case " $* " in
        *" --branch ${STUB_LIST_BRANCH} "*|*" --branch ${STUB_LIST_BRANCH}") ;;
        *" --branch "*) printf 'null\n'; exit 0 ;;
      esac
    fi
    _EMIT_MACHINE=1
    # ===== DEPTH MODEL: THE TARGET ROW SITS BELOW `--limit` NEWER ROWS (#3654 round 3) =====
    # `roborev list` has no offset — `--limit` is its only depth control — so a job on a busy
    # branch is reachable ONLY by asking for more rows. With STUB_LIST_DEPTH=N the daemon holds N
    # newer filler jobs ahead of the target, and returns the newest min(limit, N) of them, adding
    # the target only when the caller asked deeper than N. A stub that returned the target at any
    # limit would make a depth case pass before AND after the fix, i.e. measure nothing.
    if [ -n "${STUB_LIST_DEPTH:-}" ]; then
      _limit=50 _prev=""
      for _a in $*; do
        [ "$_prev" = "--limit" ] && _limit=$_a
        _prev=$_a
      done
      printf '['
      _i=0
      while [ "$_i" -lt "$STUB_LIST_DEPTH" ] && [ "$_i" -lt "$_limit" ]; do
        [ "$_i" -eq 0 ] || printf ','
        printf '{"id":%s,"git_ref":"dead..beef","status":"done","source_machine_id":"filler-not-the-target"}' \
          "$((900000 + _i))"
        _i=$((_i + 1))
      done
      # STUB_LIST_OMIT_TARGET models a job that is simply NOT on this branch's list at any depth,
      # which is what makes the exhaustion path testable: without it the target reappears as soon
      # as the limit exceeds the filler count, and the "searched to the end" case measures nothing.
      if [ "$_limit" -gt "$STUB_LIST_DEPTH" ] && [ -z "${STUB_LIST_OMIT_TARGET:-}" ]; then
        [ "$_i" -eq 0 ] || printf ','
        emit_job_object
      fi
      printf ']\n'
      exit 0
    fi
    printf '['; emit_job_object; printf ']\n'
    exit 0
    ;;
  *) printf 'stub: unsupported roborev subcommand: %s\n' "$cmd" >&2; exit 64 ;;
esac
STUB
chmod +x "$stubbin/roborev"

# ===== THE `gh` STUB (#3312 owner ruling (4)) =====
# The wrapper reads the absence waiver from ONE call — `gh pr view --json comments --jq '<program>'` —
# so the stub reproduces that call's OUTPUT rather than re-implementing jq: one line per comment,
# `<login><TAB><body, newlines flattened to spaces>`. `STUB_GH_RC` makes it exit non-zero, which is the
# single state covering "no PR for this branch", "no auth" and "API error" — all of which must leave the
# absence FAILing.
# ===== THE `gh` STUB RETURNS JSON, because the wrapper now decides STRUCTURALLY (#3312 job 26) =====
# The wrapper asks for raw `gh pr view --json comments` and hands the JSON to
# scripts/flow/roborev-waiver-scan.py, so author and body stay separate FIELDS all the way to the
# decision — there is no in-band delimiter to forge. The stub therefore has to produce that shape.
#
# FIXTURE AUTHORING: `STUB_GH_COMMENTS` keeps the compact `<SOH><login>` + body-lines form, which the
# stub CONVERTS to JSON. That form is a convenience of the test double, NOT a channel the wrapper reads.
# A case that needs a body containing a literal SOH line — the forgery fixture — sets
# `STUB_GH_COMMENTS_JSON` and passes JSON through verbatim, so nothing in the fixture layer can turn a
# forged body line into a separate comment and accidentally test the harness instead of the code.
cat >"$stubbin/gh" <<'GHSTUB'
#!/usr/bin/env bash
printf 'gh %s\n' "$*" >>"${STUB_INVOKED:-/dev/null}"
if [ "${STUB_GH_RC:-0}" -ne 0 ]; then
  printf 'gh: simulated failure\n' >&2
  exit "${STUB_GH_RC}"
fi
# ===== `gh issue view`: THE RETRIEVABILITY LEG OF THE DEFERRAL DISPOSITION (#3626) =====
# A findings deferral must name a FILED issue, so retrievability is modelled EXPLICITLY:
# STUB_GH_ISSUES lists the issue numbers that exist. It defaults to EMPTY, so a case that wants a
# grant has to SAY which issues are retrievable — the fail-closed direction, and it stops a case
# passing because the test double happened to be permissive about a question the wrapper asks.
#
# THREE-VALUED, BECAUSE THE REAL `gh` IS (#3626, lead condition 1). `gh issue view` exits 1 for a
# missing issue AND for an auth/network failure, so the two are told apart by the DIAGNOSTIC and the
# stub reproduces the REAL TEXT of each, measured on gh 2.98.0:
#   not found : GraphQL: Could not resolve to an issue or pull request with the number of N. (repository.issue)
#   no auth   : HTTP 401: Bad credentials (https://api.github.com/graphql)
# STUB_GH_ISSUE_ERR overrides the diagnostic (and forces exit 1) so a COULD-NOT-ASK can be fixtured
# without also disabling `gh pr view`, which STUB_GH_RC would do — the deferral would then never reach
# the retrievability leg at all and the case would pass for the wrong reason.
if [ "${1:-}" = "issue" ] && [ "${2:-}" = "view" ]; then
  if [ -n "${STUB_GH_ISSUE_ERR:-}" ]; then
    printf '%s\n' "$STUB_GH_ISSUE_ERR" >&2
    exit 1
  fi
  # ===== AND `state` IS MODELLED, BECAUSE A CLOSED ISSUE ANSWERS AND EXITS 0 (#3626 round 3) =====
  # STUB_GH_ISSUES lists OPEN issues; STUB_GH_ISSUES_CLOSED lists CLOSED ones. The real `gh` returns
  # the number and exits 0 for both, which is exactly why a number-only test could not enforce
  # not-dropped — so the stub must be able to produce that shape or the case measures nothing.
  for _stub_known in ${STUB_GH_ISSUES:-} ${STUB_GH_ISSUES_CLOSED:-}; do
    [ "$_stub_known" = "${3:-}" ] || continue
    _stub_state=OPEN
    for _stub_closed in ${STUB_GH_ISSUES_CLOSED:-}; do
      [ "$_stub_closed" = "$_stub_known" ] && _stub_state=CLOSED
    done
    # THE ANSWER FOLLOWS THE FIELDS THE CALLER ASKED FOR, as the real `gh` does. That matters for more
    # than fidelity: the (df7e) mutant asks the OLD `--json number --jq .number` shape, and a stub that
    # always appended a state would make the mutant fail for a reason that was never in question —
    # a contrast that establishes nothing.
    case " $* " in
      *" --jq "*[Ss]tate*|*" state"*|*",state"*)
        printf '%s %s\n' "$_stub_known" "$_stub_state" ;;
      *" --jq "*) printf '%s\n' "$_stub_known" ;;
      *) printf '{"number":%s,"state":"%s"}\n' "$_stub_known" "$_stub_state" ;;
    esac
    exit 0
  done
  printf 'GraphQL: Could not resolve to an issue or pull request with the number of %s. (repository.issue)\n' "${3:-}" >&2
  exit 1
fi
if [ -n "${STUB_GH_COMMENTS_JSON:-}" ]; then
  printf '%s\n' "$STUB_GH_COMMENTS_JSON"
  exit 0
fi
if [ -n "${STUB_GH_COMMENTS_FILE:-}" ] && [ -f "${STUB_GH_COMMENTS_FILE}" ]; then
  STUB_GH_SRC=$(cat "${STUB_GH_COMMENTS_FILE}")
else
  STUB_GH_SRC=$(printf '%b' "${STUB_GH_COMMENTS:-}")
fi
printf '%s' "$STUB_GH_SRC" | python3 -c '
import json, sys
raw = sys.stdin.read()
comments = []
author = None
body = []
for line in raw.split("\n"):
    if line.startswith("\u0001"):
        if author is not None:
            comments.append({"author": {"login": author}, "body": "\n".join(body)})
        author = line[1:]
        body = []
    elif author is not None:
        body.append(line)
if author is not None:
    comments.append({"author": {"login": author}, "body": "\n".join(body)})
# COMMENTS ONLY, exactly as `gh pr view --json comments` returns them. NO `body` FIELD IS
# SYNTHESISED (#3626): the PR-body link check was deleted rather than patched — a body is editable at
# any time by anyone with write access with no per-edit attribution, while a comment is permanent and
# attributable — so a stub that still offered a body would model a channel the code does not read.
json.dump({"comments": comments}, sys.stdout)
'
exit 0
GHSTUB
chmod +x "$stubbin/gh"

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
#   --- the #3229 docs-census family -----------------------------------------------
#   docs-executables  .sh/.py/.bt under docs/reports/x-artifacts/harness/ — the PR
#                   #3222 shape: 100% under docs/, 100% executable CODE
#   docs-prose      markdown only, UNDER docs/ (distinct from docs-only, which puts
#                   its markdown at the repo root)
#   docs-artifacts  markdown + declared docs-scoped artifacts (.txt/.json/.log/.err/
#                   .jsonl) under docs/reports/x-artifacts/ — still code-free
#   docs-odd-name   a .sh under docs/ whose filename carries SPACES and a literal
#                   double quote — the NUL-safety regression (`git diff --numstat`
#                   C-QUOTES it, `-z` does not)
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
#   docs-extensionless-exec  an EXTENSIONLESS 100755 file under docs/reports/x-artifacts/,
#                   beside a `.md`, an extensionless 100644 file, and a NON-executable `.sh`.
#                   The real repo tracks three such executables (`ws0-readbw`, `ws0-stream`,
#                   `offcputime-bigmap`); the retired prefix-only rule classified every
#                   extensionless docs/ path non-code, so `prompt-content:` asserted NOTHING
#                   about exactly the class AC2 names
#   docs-extensionless-plain  THE ONE-VARIABLE SIBLING: the SAME path as above at mode
#                   100644, beside a .rs change. Same name, same directory, only the recorded
#                   mode differs — so a green pair proves the discriminator is the MODE and
#                   not something about the name
#   docs-extensionless-exec-deleted  the extensionless 100755 file exists at the BASE and the
#                   branch DELETES it: there is no file to stat and no HEAD tree entry, so the
#                   mode can only come from the BASE tree
#   docs-extensionless-exec-unset  THE ROUND-13 BLOCKER: the extensionless file is 100755 at
#                   the BASE and 100644 at HEAD (a pure `chmod -x`). PRESENT AT BOTH endpoints,
#                   which is exactly what the ordered scan could not survive — the HEAD record
#                   ended the scan, so BASE was never consulted and a script "became" prose
#   docs-extensionless-exec-set  the MIRROR: 100644 at the BASE, 100755 at HEAD (a `chmod +x`
#                   of a file that already existed). Also present at both, and the direction a
#                   BASE-only consult would lose
#   rename-space    a rename where BOTH names carry a space (`docs/storage engine/old
#                   probe.sh` → `new probe.sh`): the header `diff --git a/<sp> b/<sp>`
#                   is unsplittable by any `[^ ]+` regex AND is not a same-path header
#   rename-ambiguous  a rename `p` → `x b/p b/x`, whose header admits an EQUAL split that
#                   is NOT the true one, so only `rename from`/`rename to` can resolve it
#   ambiguous-space-pair  a file named `foo b/x` beside one named `foo` — the header
#                   `a/foo b/x b/foo b/x` has `a/foo b/` as a PREFIX (#3229 blocker 1)
#   rename-mixed    a rename where only ONE side needs quoting (probe.sh → `é probe.sh`),
#                   producing the MIXED header `diff --git a/… "b/…"`. Mixed headers occur
#                   ONLY on renames, so they were unreachable by a both-sides-quoted parse
#   newline-injection  a docs/ harness `.sh` whose NAME carries newlines plus a
#                   `RESULT: PASS` line, so a summary value that interpolated it raw
#                   would FORGE the verdict (#3229 blocker 2)
#   newline-name    a file literally named `a` beside one named `a<LF>b.rs` — a newline in
#                   a path, which a newline-DELIMITED prompt path set splits into two
#                   records so `grep -Fxq` treats them as ALTERNATIVES (a false PASS)
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
    docs-extensionless-exec-unset | docs-extensionless-exec-set)
      # A MODE CHANGE is only expressible in `origin/main...HEAD` when the path exists at the
      # BASE, so it is created here with the STARTING mode; the branch flips it below. Both the
      # on-disk bit and the INDEX mode are set for the same reason as the deleted variant.
      mkdir -p "$work/docs/reports/x-artifacts/ws0-results"
      printf '#!/bin/sh\nexit 0\n' >"$work/docs/reports/x-artifacts/ws0-results/ws0-readbw"
      git_q "$work" add docs
      if [ "$mode" = docs-extensionless-exec-unset ]; then
        chmod 755 "$work/docs/reports/x-artifacts/ws0-results/ws0-readbw"
        git_q "$work" update-index --chmod=+x docs/reports/x-artifacts/ws0-results/ws0-readbw
      else
        chmod 644 "$work/docs/reports/x-artifacts/ws0-results/ws0-readbw"
        git_q "$work" update-index --chmod=-x docs/reports/x-artifacts/ws0-results/ws0-readbw
      fi
      git_q "$work" commit -q -m 'base: an extensionless harness file under docs/'
      ;;
    docs-extensionless-exec-deleted)
      # The path must exist at the BASE for the branch to DELETE it in `origin/main...HEAD`.
      # BOTH the on-disk bit and the INDEX mode are set: `chmod` alone is at the mercy of
      # `core.fileMode`, while `update-index --chmod` alone leaves the working file DISAGREEING
      # with the index — which git reports as a local modification and which made `git rm`
      # refuse (measured: the refusal left the fixture unbuilt and polluted make_fixture's
      # stdout, so every assert in the case reported against a non-existent work tree).
      mkdir -p "$work/docs/reports/x-artifacts/ws0-results"
      printf '#!/bin/sh\nexit 0\n' >"$work/docs/reports/x-artifacts/ws0-results/ws0-readbw"
      chmod 755 "$work/docs/reports/x-artifacts/ws0-results/ws0-readbw"
      git_q "$work" add docs
      git_q "$work" update-index --chmod=+x docs/reports/x-artifacts/ws0-results/ws0-readbw
      git_q "$work" commit -q -m 'base: an extensionless harness executable under docs/'
      ;;
    rename-ambiguous)
      # The OTHER direction of the header ambiguity (#3229 blocker 1): a rename whose
      # header `diff --git a/p b/x b/p b/x` admits an EQUAL split (`p b/x` == `p b/x`)
      # that is NOT the true one. Equality alone would therefore report BOTH real sides
      # ABSENT; only the `rename from`/`rename to` lines can resolve it.
      printf 'plain\n' >"$work/p"
      git_q "$work" add p
      git_q "$work" commit -q -m 'base: a one-character path'
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
    docs-odd-name)
      mkdir -p "$work/docs/reports/x-artifacts/harness"
      printf '#!/bin/sh\nexit 0\n' >"$work/docs/reports/x-artifacts/harness/odd \"q\" name.sh"
      printf 'fn helper() {}\n' >>"$work/main.rs"
      git_q "$work" add docs main.rs
      git_q "$work" commit -q -m 'a docs harness path with spaces and a quote'
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
      # (correctly) removes it from the reviewer's diff, so `prompt-content:` demanded a
      # file the configuration had already excluded ⇒ a false FAIL on an ordinary docs+code
      # branch.
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
    docs-extensionless-exec)
      # FOUR paths, chosen so ONE `prompt-content:` count discriminates all four rules:
      #   ws0-readbw   extensionless, 100755   -> CODE   (the class this fixture exists for)
      #   plain.sh     `.sh`,         100644   -> CODE   (a code EXTENSION wins over the mode;
      #                                                   the mode is consulted ONLY when there
      #                                                   is no extension)
      #   x-report.md  `.md`                   -> non-code (unchanged)
      #   NOTICE       extensionless, 100644   -> non-code (what the prefix list is FOR)
      # So the code census is exactly 2, and a `2/2` can only be those two.
      mkdir -p "$work/docs/reports/x-artifacts/ws0-results" "$work/docs/reports/x-artifacts/harness"
      printf '#!/bin/sh\nexit 0\n' >"$work/docs/reports/x-artifacts/ws0-results/ws0-readbw"
      printf '#!/bin/sh\nexit 0\n' >"$work/docs/reports/x-artifacts/harness/plain.sh"
      printf '# report\n' >"$work/docs/reports/x-report.md"
      printf 'no rights reserved\n' >"$work/docs/NOTICE"
      # On-disk bits AND index modes (see the deleted-variant comment): a `chmod` alone bends
      # to `core.fileMode`, an `update-index --chmod` alone leaves the file reported modified.
      chmod 755 "$work/docs/reports/x-artifacts/ws0-results/ws0-readbw"
      chmod 644 "$work/docs/reports/x-artifacts/harness/plain.sh" "$work/docs/NOTICE"
      git_q "$work" add docs
      git_q "$work" update-index --chmod=+x docs/reports/x-artifacts/ws0-results/ws0-readbw
      git_q "$work" update-index --chmod=-x docs/reports/x-artifacts/harness/plain.sh
      git_q "$work" update-index --chmod=-x docs/NOTICE
      git_q "$work" commit -q -m 'an extensionless executable under docs/, beside prose'
      ;;
    docs-extensionless-plain)
      # The SAME path at mode 100644. Nothing but the recorded mode differs from the fixture
      # above, so the pair is a one-variable control: if the classifier were keying on the
      # directory or the name, both would land in the same class.
      mkdir -p "$work/docs/reports/x-artifacts/ws0-results"
      printf 'plain notes with no extension\n' >"$work/docs/reports/x-artifacts/ws0-results/ws0-readbw"
      chmod 644 "$work/docs/reports/x-artifacts/ws0-results/ws0-readbw"
      printf 'fn helper() {}\n' >>"$work/main.rs"
      git_q "$work" add docs main.rs
      git_q "$work" update-index --chmod=-x docs/reports/x-artifacts/ws0-results/ws0-readbw
      git_q "$work" commit -q -m 'an extensionless NON-executable under docs/, beside code'
      ;;
    docs-extensionless-exec-deleted)
      git_q "$work" rm -q docs/reports/x-artifacts/ws0-results/ws0-readbw
      git_q "$work" commit -q -m 'delete the extensionless harness executable'
      ;;
    docs-extensionless-exec-unset | docs-extensionless-exec-set)
      # A PURE mode change — no content edit — so `--numstat` reports `0 0 <path>` and the ONLY
      # thing distinguishing the two endpoints is the recorded mode. A `.md` rides along so the
      # census has a second, provably NON-code file and the `1/1` count below is discriminating.
      if [ "$mode" = docs-extensionless-exec-unset ]; then
        chmod 644 "$work/docs/reports/x-artifacts/ws0-results/ws0-readbw"
        git_q "$work" update-index --chmod=-x docs/reports/x-artifacts/ws0-results/ws0-readbw
      else
        chmod 755 "$work/docs/reports/x-artifacts/ws0-results/ws0-readbw"
        git_q "$work" update-index --chmod=+x docs/reports/x-artifacts/ws0-results/ws0-readbw
      fi
      printf '# report\n' >"$work/docs/reports/x-report.md"
      git_q "$work" add docs/reports/x-report.md
      git_q "$work" commit -q -m 'flip the recorded mode of an extensionless harness file'
      ;;
    rename-space)
      git_q "$work" mv "docs/storage engine/old probe.sh" "docs/storage engine/new probe.sh"
      git_q "$work" commit -q -m 'rename a space-bearing harness script'
      ;;
    rename-mixed)
      git_q "$work" mv "docs/reports/x-artifacts/probe.sh" "docs/reports/x-artifacts/é probe.sh"
      git_q "$work" commit -q -m 'rename an ASCII script to a non-ASCII name'
      ;;
    rename-ambiguous)
      mkdir -p "$work/x b/p b"
      git_q "$work" mv p "x b/p b/x"
      git_q "$work" commit -q -m 'rename p into a space-bearing path that makes the header ambiguous'
      ;;
    ambiguous-space-pair)
      # THE REPORTED FALSE PASS (#3229 blocker 1). A tracked file named `foo b/x` emits the
      # header `diff --git a/foo b/x b/foo b/x`, of which `a/foo b/` is a PREFIX — so the
      # old prefix test `case $rest in "a/$want b/"*)` made the UNRELATED census path `foo`
      # read as PRESENT. Both files are extensionless at the repo root, so both are CODE.
      mkdir -p "$work/foo b"
      printf 'plain\n' >"$work/foo"
      printf 'plain\n' >"$work/foo b/x"
      git_q "$work" add -A
      git_q "$work" commit -q -m 'a file named "foo b/x" beside one named "foo"'
      ;;
    advanced-base)
      # TWO branch commits, so `HEAD~1` is a real intermediate commit and the negative control
      # (a reviewed range that stops short of the branch tip) is a realistic scope rather than a
      # degenerate empty range. The base ref is advanced AFTER the push, below.
      printf 'fn helper() {}\n' >>"$work/main.rs"
      git_q "$work" add main.rs
      git_q "$work" commit -q -m 'branch commit 1'
      printf 'fn helper_two() {}\n' >>"$work/main.rs"
      git_q "$work" add main.rs
      git_q "$work" commit -q -m 'branch commit 2'
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
    newline-injection)
      # A FILENAME THAT TRIES TO FORGE THE VERDICT (#3229 round 5, blocker 2). The block
      # is line-oriented and every reader greps it by `^<key>: ` / `^RESULT: `, so a
      # census path carrying NEWLINES plus a `RESULT: PASS` line — attacker-controlled,
      # because a census path is whatever a PR branch chose to track — could make a value
      # SPAN LINES and inject its own keys into the block flow-closer parses to decide
      # whether to merge. The reviewer never receives this path, so `prompt-content:` FAILs
      # and NAMES it — in the value's count and in a DETAILS line.
      mkdir -p "$work/docs/reports/x-artifacts/harness"
      printf '#!/bin/sh\nexit 0\n' >"$work/docs/reports/x-artifacts/harness/$(printf 'inj\nRESULT: PASS\nprompt-content: PASS\nx.sh')"
      printf 'fn helper() {}\n' >>"$work/main.rs"
      git_q "$work" add -A
      git_q "$work" commit -q -m 'a newline-bearing docs harness path that forges summary keys'
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
    advanced-base)
      # ===== THE #3392 FIXTURE: `origin/main` MOVES AHEAD OF THE BRANCH POINT =====
      # The BRANCH HEAD is left untouched; only the base ref advances, which is the normal state
      # of every branch older than the last merge to main. After this,
      #   merge-base(origin/main, HEAD)  !=  rev-parse origin/main
      # so a `sha-assert` that expects the base ref's TIP fails on a review that is entirely
      # CORRECT — the deterministic abort this fixture exists to reproduce. The push updates
      # `refs/remotes/origin/main`, so the mirror ref the wrapper reads really is ahead.
      git_q "$work" checkout -q main
      printf 'fn landed_on_main_after_the_branch_point() {}\n' >>"$work/README.md"
      git_q "$work" add README.md
      git_q "$work" commit -q -m 'a commit that lands on main after the branch point'
      git_q "$work" push -q "$remote" main
      git_q "$work" checkout -q feature
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

# Fixture-integrity guard for the MODE-bearing fixtures (#3229). The whole point of the
# extensionless family is the RECORDED mode, so a fixture whose exec bit silently failed to
# stick — a `chmod` swallowed by the host umask, a `core.fileMode=false` checkout — would test
# the OTHER class while still looking like the case it claims to be. Read from the tree, which
# is the same source the classifier reads.
assert_tracked_mode() { # assert_tracked_mode <label> <work> <ref> <path> <want-mode>
  local got
  got=$(git -C "$2" ls-tree -z "$3" -- ":(literal)$4" 2>/dev/null | cut -d' ' -f1)
  if [ "$got" = "$5" ]; then
    ok "$1: $4 is recorded $5 in $3"
  else
    bad "$1: $4 is recorded '${got:-<absent>}' in $3, want $5 — the fixture tests a different class than it claims"
  fi
}

# range_ref <work> [base-ref]: the git_ref shape a RANGE review records, "<base40>..<head40>".
# THE BASE ENDPOINT IS THE MERGE-BASE, not `rev-parse <base-ref>` (#3392): roborev reviews
# `<base>...HEAD`, i.e. `merge-base(<base>, HEAD)..HEAD`, and that is what its job record carries.
# In a fixture whose base ref has not advanced past the branch point the two are the SAME commit,
# so the old spelling still passed — it was simply describing the wrong thing, and it would have
# silently mis-stated the record for any fixture that DOES advance the base (see the `advanced-base`
# fixture and the `mb*` family).
range_ref() { printf '%s..%s' "$(git -C "$1" merge-base "${2:-origin/main}" HEAD)" "$(git -C "$1" rev-parse HEAD)"; }

# assert_base_advanced <label> <work>: the FIXTURE-INTEGRITY guard for every case that exists to
# distinguish the merge-base from the base ref's TIP. If the fixture stopped advancing the base ref
# the two collapse to one commit and every such case would pass while testing NOTHING — the exact
# vacuity this suite exists to prevent — so the inequality is asserted explicitly rather than assumed.
assert_base_advanced() { # assert_base_advanced <label> <work>
  local tip mb
  tip=$(git -C "$2" rev-parse origin/main 2>/dev/null || printf '')
  mb=$(git -C "$2" merge-base origin/main HEAD 2>/dev/null || printf '')
  if [ -n "$tip" ] && [ -n "$mb" ] && [ "$tip" != "$mb" ]; then
    ok "$1: fixture is NON-VACUOUS — the origin/main tip ($tip) and the merge-base ($mb) are different commits"
  else
    bad "$1: fixture is VACUOUS — origin/main tip '${tip:-<unresolved>}' and merge-base '${mb:-<unresolved>}' are not two different commits, so nothing about the distinction is under test"
  fi
}

# The wrapper's own temp directory for fixture runs. It MUST exist: the census's mode probe uses `mktemp`
# under $TMPDIR, and a missing directory makes that probe fail — which the census correctly reports as an
# UNMEASURABLE mode and fails closed on, i.e. every fixture run would abort pre-enqueue (measured).
WRAPPER_TMP="$tmp/wrapper-tmp"
mkdir -p "$WRAPPER_TMP"

CASE_N=0
OUT=""
RC=0
INVOKED=""
# A throwaway HOME for every wrapper run (see run_wrapper).
FIXTURE_HOME="$tmp/home"
mkdir -p "$FIXTURE_HOME"
# An OPTIONAL extra PATH element prepended AHEAD of the stub dir for a single case, so a case can
# FAULT-INJECT one git subcommand (#3229 round-14: `git ls-tree` failing is the only way to reach
# the UNMEASURABLE state end-to-end). Empty for every other case, and every user restores it.
WRAPPER_PATH_PREFIX=""
# An OPTIONAL TMPDIR override for the single case that must exercise a hostile TMPDIR (one resolving
# INSIDE the reviewed repo). Every other run gets the per-process capture parent.
WRAPPER_TMPDIR=""

run_wrapper() { # run_wrapper [--wrapper <path>] <work-dir> [extra wrapper args...]
  # THE SCRATCH-COPY PATH IS A PARAMETER, NOT A GLOBAL (roborev job 58). It was
  # `${RUN_WRAPPER_PATH:-$WRAPPER_REAL}`, and guarding that global meant counting its assignments --
  # which matched only `NAME=`, so `export`/`declare`/`printf -v`/`read` evaded it. That is the
  # assignment-syntax enumeration roborev has now corrected me on THREE times (WRAPPER_REAL job 37,
  # WRAPPER job 54, this). A `local` cannot be written from outside the function, so the third
  # instance is not fixed but UNEXPRESSIBLE -- the same move that retired the WRAPPER global.
  local _rw_wrapper="$WRAPPER_REAL"
  if [ "${1:-}" = "--wrapper" ]; then _rw_wrapper="$2"; shift 2; fi
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
  # The observer writes beside the transcript, so the stub is told which record to wait on (FIX 4).
  WRAPPER_LOG_PATH="$tmp/transcript-$CASE_N.txt"
  for _rw_i in $(seq 1 $#); do
    if [ "${!_rw_i}" = "--log" ]; then
      _rw_next=$((_rw_i + 1))
      WRAPPER_LOG_PATH="${!_rw_next}"
    fi
  done
  : >"$INVOKED"
  # The sanctioned invocation reviews the RANGE <base>..HEAD, so the job record's
  # git_ref is "<base40>..<head40>". Default the stub to the correct range unless the
  # case pinned git_ref itself (or asked for it to be absent with `none`).
  if [ -z "${STUB_GIT_REF:-}" ]; then STUB_GIT_REF=$(range_ref "$work"); fi
  # The job row reports the branch it was enqueued for, and the daemon lists that branch's jobs —
  # which, for an ordinary run, is the fixture's own branch (#3654 round 2). Defaulted here, beside
  # STUB_GIT_REF, for the same reason: a case pins it only when the DIVERGENCE is the subject.
  if [ -z "${STUB_JOB_BRANCH:-}" ]; then STUB_JOB_BRANCH=$(git -C "$work" rev-parse --abbrev-ref HEAD); fi
  if [ -z "${STUB_LIST_BRANCH:-}" ]; then STUB_LIST_BRANCH="$STUB_JOB_BRANCH"; fi
  # HOME is redirected to a throwaway directory: nothing in the wrapper reads a roborev
  # config any more (#3283), but HERMETICITY is asserted structurally at the bottom of this
  # file and a host `$HOME/.roborev/` must never be able to influence a fixture run.
  STUB_INVOKED="$INVOKED" PATH="${WRAPPER_PATH_PREFIX:+$WRAPPER_PATH_PREFIX:}$stubbin:$PATH" HOME="$FIXTURE_HOME" \
    TMPDIR="${WRAPPER_TMPDIR:-$WRAPPER_TMP}" \
    bash "$_rw_wrapper" --repo "$work" --agent codex --model gpt-5.6-sol \
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

# ===== NO EMITTED DIAGNOSTIC CARRIES ANY PART OF EITHER MARKER FORM (#3312 job 23 / #3626) =====
# ATTACHED TO EVERY DIAGNOSTIC-EMITTING CASE, which is the whole point of it being a helper. It used
# to be attached to ONE case — the `NONE` state, where the property holds TRIVIALLY because no marker
# was ever parsed — while the MALFORMED states, the only ones whose detail was BUILT FROM the marker
# pattern, asserted their cause text and nothing else. That is why a green run could not see the leak:
# the scanner's MALFORMED detail quoted the whole required form, the summary key interpolated it, and
# every assert pointed somewhere else. A property asserted only where it cannot fail is not asserted.
assert_no_marker_form() { # assert_no_marker_form <case-label>
  assert_lacks "$1: the output carries no part of the absence-waiver marker" 'roborev-waive'
  assert_lacks "$1: the output carries no part of the findings-deferral marker" 'roborev-defer'
  assert_lacks "$1: nor a field skeleton that would make a pasted block fillable" 'base=<40-hex>'
}

# THE BLOCK CARRIES EXACTLY ONE VERDICT LINE (#3229 blocker 2). `assert_verdict` reads
# `^RESULT: ` | tail -1, so it cannot see an INJECTED verdict line above the real one —
# only a count can.
assert_one_result_line() { # assert_one_result_line <label>
  local n
  n=$(grep -cE '^RESULT: ' "$OUT" || true)
  if [ "$n" -eq 1 ]; then
    ok "$1: exactly one RESULT: line"
  else
    bad "$1: $n 'RESULT:' lines (want 1) — a value or DETAILS line spans lines"
    printf -- '------- captured -------\n'; cat "$OUT"; printf -- '------------------------\n'
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

# THE POSITIVE CONTROL for the assert above (#3229 round-10). `assert_never_enqueued` is
# satisfied by an EMPTY witness file, so it also "passes" when the harness never ran the
# wrapper at all — the failure direction that looks like success. A case pinning a
# pre-enqueue FAIL is only evidence when its sibling case, differing in ONE variable, is
# shown to reach the enqueue.
assert_enqueued() { # assert_enqueued <label>
  if [ -s "$INVOKED" ]; then
    ok "$1: a review WAS enqueued (the pre-enqueue path was really passed)"
  else
    bad "$1: NO review was enqueued, so the run stopped pre-enqueue — an 'it was blocked' assert elsewhere would be satisfied by a harness that never ran"
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
export STUB_LIST_DEPTH=''
export STUB_LIST_OMIT_TARGET=''
export STUB_REVIEW_RC=0
export STUB_ANNOUNCE_SHA=''
# #3312 (owner ruling (4)): the WAIVER knobs. There are no snapshot knobs any more — nothing reads a
# snapshot and nothing classifies delivery mode, so the lifecycle fixtures went with the machinery.
# STUB_GH_COMMENTS is what the `gh` stub prints for `gh pr view --json comments`: one line per comment,
# `<login><TAB><flattened body>`, exactly the shape the wrapper's `--jq` produces. STUB_GH_RC makes the
# `gh` call FAIL, which is how "no PR / no auth / API error" is exercised.
export STUB_RECORD_OUTPUT=''
export STUB_RECORD_OUTPUT_FIELD=''
export STUB_GH_COMMENTS=''
export STUB_GH_COMMENTS_JSON=''
export STUB_GH_COMMENTS_FILE=''
export STUB_GH_RC=0
# #3626: the FINDINGS-DEFERRAL knobs. STUB_GH_ISSUES is the set of issue numbers that EXIST, so
# retrievability is a fixture decision and not a stub default; STUB_GH_ISSUE_ERR fixtures a
# COULD-NOT-ASK by giving `gh issue view` a diagnostic that does NOT say the issue is missing.
# There is deliberately no STUB_PR_BODY: the PR body is read by nothing (see the stub above).
export STUB_GH_ISSUES=''
export STUB_GH_ISSUES_CLOSED=''
export STUB_GH_ISSUE_ERR=''
export STUB_ON_REVIEW=''
# #3654: the DAEMON-ID knob. Empty by default, and the stub emits it on the `list` payload ONLY,
# which is where roborev v0.61.2 actually carries `source_machine_id`.
export STUB_SOURCE_MACHINE_ID=''
# #3654 round 2: the BRANCH knobs. STUB_JOB_BRANCH is the branch the job row reports as its own
# (`none` suppresses the field); STUB_LIST_BRANCH is the branch the daemon's list will answer for.
# Both default, in run_wrapper, to the fixture's own current branch — the ordinary case — so a case
# that wants them to DIVERGE has to say so.
export STUB_JOB_BRANCH=''
export STUB_LIST_BRANCH=''
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
  STUB_LIST_JSON=array
  STUB_LIST_DEPTH=''
  STUB_LIST_OMIT_TARGET=''
  STUB_RECORD_OUTPUT=''
  STUB_RECORD_OUTPUT_FIELD=''
  STUB_GH_COMMENTS=''
  STUB_GH_COMMENTS_JSON=''
  STUB_GH_COMMENTS_FILE=''
  STUB_GH_RC=0
  STUB_GH_ISSUES=''
  STUB_GH_ISSUES_CLOSED=''
  STUB_GH_ISSUE_ERR=''
  STUB_ON_REVIEW=''
  # No daemon id by default: the FAIL-CLOSED direction for the `job-machine:` key (#3654) — a case
  # that wants the uuid arm has to SAY so, so no case passes because the double happened to answer.
  STUB_SOURCE_MACHINE_ID=''
  STUB_JOB_BRANCH=''
  STUB_LIST_BRANCH=''
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
# The (cx*) family (issue #3229): the CENSUS CLASSIFICATION and the PROMPT-CONTENT
# match — what this repo's diff actually contains, and what the reviewer actually
# received.
#
# What they pin: under the retired `docs/**` configuration, 33 EXECUTABLE harness
# files were dropped from PR #3222's review while every fixture stayed green. The
# shipped remedy is the NARROWED `.roborev.toml` (artifact extensions inside
# artifact-bearing directories, never a blanket `docs/**`) plus these cases, which
# assert that an executable under `docs/` is classified CODE, is not code-free, and
# REACHES the reviewer's prompt — measured against the prompt actually sent.
#
# WHAT IS DELIBERATELY NOT HERE (#3283): there is no case asserting what roborev's
# exclusion set WOULD do to a given census, because there is no longer any code that
# predicts it. An oracle that did — a bash port of roborev's `git.FormatExcludeArgs`
# over a TOML parse of three config sources — was built on this issue and REMOVED by
# owner ruling: four consecutive review rounds found false-PASSes inside it at an
# INCREASING rate. A guard with known documented false-PASSes is worse than no guard,
# because it invites reliance it cannot support. The fixtures therefore no longer
# supply a `.roborev.toml` at all: nothing reads one, and an inert input that reads as
# load-bearing is the same class of misleading test.
# ===========================================================================

printf '== case (cx1): executables under docs/ are CODE, survive the narrowed config, and ARE enqueued ==\n'
reset_stub
# The PR #3222 shape: 100% of the diff under docs/, 100% of it executable.
work=$(make_fixture case_cx1 docs-executables)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_PROMPT='Review this diff:\ndiff --git a/docs/reports/x-artifacts/harness/run.sh b/docs/reports/x-artifacts/harness/run.sh\ndiff --git a/docs/reports/x-artifacts/harness/classify.py b/docs/reports/x-artifacts/harness/classify.py\ndiff --git a/docs/reports/x-artifacts/harness/offcpu.bt b/docs/reports/x-artifacts/harness/offcpu.bt'
run_wrapper "$work"
assert_verdict 'case (cx1)' PASS 0
assert_says 'case (cx1) a docs/ path prefix does not make code documentation' '^code-free: PASS$'
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
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
run_wrapper "$work"
assert_verdict 'case (cx2)' FAIL 1
assert_says 'case (cx2) code-free still FAILs on prose under docs/' '^code-free: FAIL \(code-free census: 2/2 files are documentation/specification text\)$'
assert_never_enqueued 'case (cx2)'

printf '== case (cx3): docs-scoped ARTIFACTS with no executables are still code-free ==\n'
reset_stub
# The narrowing must not have traded the old blind spot for a VACUOUS review of a diff
# roborev would empty: .txt/.json/.log/.err/.jsonl under docs/ are declared artifacts.
work=$(make_fixture case_cx3 docs-artifacts)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
run_wrapper "$work"
assert_verdict 'case (cx3)' FAIL 1
assert_says 'case (cx3) artifacts under docs/ classify non-code' '^code-free: FAIL \(code-free census: 6/6 files are documentation/specification text\)$'
assert_never_enqueued 'case (cx3)'

printf '== case (cx3a): an EXTENSIONLESS EXECUTABLE under docs/ is CODE and IS expected in the prompt ==\n'
reset_stub
# THE ROUND-11 BLOCKER. `CODE_FREE_EXTENSIONLESS_PREFIXES` used to make every extensionless
# path under `docs/` non-code, so it never entered `census_code_paths` and `prompt-content:`
# made NO CLAIM about it — while the narrowed `exclude_patterns` (only `*.md` globally plus
# docs-scoped ARTIFACT EXTENSIONS) do not exclude it, so it genuinely reaches the reviewer.
# The guard was silent on precisely the class AC2's trigger names, and three files in this
# repo have that exact shape today (`ws0-readbw`, `ws0-stream`, `offcputime-bigmap`, all
# 100755). The count is the assertion: 2 of the 4 census files are CODE, and only the
# extensionless EXECUTABLE and the non-executable `.sh` can be them.
work=$(make_fixture case_cx3a docs-extensionless-exec)
assert_tracked_mode 'case (cx3a) fixture' "$work" HEAD docs/reports/x-artifacts/ws0-results/ws0-readbw 100755
assert_tracked_mode 'case (cx3a) fixture' "$work" HEAD docs/reports/x-artifacts/harness/plain.sh 100644
assert_tracked_mode 'case (cx3a) fixture' "$work" HEAD docs/NOTICE 100644
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_PROMPT='Review this diff:\ndiff --git a/docs/reports/x-artifacts/ws0-results/ws0-readbw b/docs/reports/x-artifacts/ws0-results/ws0-readbw\ndiff --git a/docs/reports/x-artifacts/harness/plain.sh b/docs/reports/x-artifacts/harness/plain.sh'
run_wrapper "$work"
assert_verdict 'case (cx3a)' PASS 0
assert_says 'case (cx3a) the census saw all four files' '^census: 4 files, \+[0-9]+/-[0-9]+$'
assert_says 'case (cx3a) an extensionless executable makes the diff reviewable' '^code-free: PASS$'
assert_says 'case (cx3a) exactly the two CODE paths are expected, and both arrived' '^prompt-content: PASS \(2/2 code census paths present\)$'
# The two NON-code paths are still non-code: were `.md` or the extensionless 100644 `NOTICE`
# counted, the value would read 3/4 or 4/4 — neither of which this case's prompt could satisfy.
assert_lacks 'case (cx3a) the .md and the extensionless non-executable are NOT in the code census' '^prompt-content: (PASS|FAIL) \([0-9]+/(3|4) '
assert_enqueued 'case (cx3a)'

printf '== case (cx3b): the SAME extensionless path, ABSENT from the prompt, is a FAIL that NAMES it ==\n'
reset_stub
# The other direction, and the one that makes (cx3a) mean something: before the fix this diff
# passed with `prompt-content: PASS (1/1 ...)` — the reviewer's coverage of `ws0-readbw` was
# never in question because the guard had already dropped it from the subject set.
work=$(make_fixture case_cx3b docs-extensionless-exec)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_PROMPT='Review this diff:\ndiff --git a/docs/reports/x-artifacts/harness/plain.sh b/docs/reports/x-artifacts/harness/plain.sh'
run_wrapper "$work"
assert_verdict 'case (cx3b)' FAIL 1
assert_says 'case (cx3b) the missing extensionless executable is one absent code path of two' '^prompt-content: FAIL \(1/2 code census paths absent from the prompt\)$'
assert_says 'case (cx3b) the absent path is NAMED' '^  docs/reports/x-artifacts/ws0-results/ws0-readbw$'
assert_lacks 'case (cx3b) never reports a PASS on a prompt missing a code path' '^prompt-content: PASS'

printf '== case (cx3c): the SAME path NON-EXECUTABLE is still non-code (one-variable control) ==\n'
reset_stub
# Identical name, identical directory, mode 100644 instead of 100755. This is what keeps the
# fix from being "extensionless under docs/ is code now": `docs/LICENSE`, `openspec/NOTES` and
# a `.claude/CODEOWNERS` must stay out of the code census, which is the only thing the prefix
# list was ever for. `1/1` — not `2/2` — is the assertion.
work=$(make_fixture case_cx3c docs-extensionless-plain)
assert_tracked_mode 'case (cx3c) fixture' "$work" HEAD docs/reports/x-artifacts/ws0-results/ws0-readbw 100644
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_PROMPT='Review this diff:\ndiff --git a/main.rs b/main.rs'
run_wrapper "$work"
assert_verdict 'case (cx3c)' PASS 0
assert_says 'case (cx3c) the census saw both files' '^census: 2 files, \+[0-9]+/-[0-9]+$'
assert_says 'case (cx3c) only the .rs file is a code census path' '^prompt-content: PASS \(1/1 code census paths present\)$'
assert_lacks 'case (cx3c) the extensionless non-executable is not demanded of the prompt' '^prompt-content: FAIL'

printf '== case (cx3d): a DELETED extensionless executable is classified from the BASE tree ==\n'
reset_stub
# There is no working-tree file to stat and no HEAD tree entry, so `test -x` would answer
# "not executable" — a different question with a plausible value — and the removal of a harness
# executable would go unasserted. The mode comes from the BASE tree instead, which is the
# fail-closed direction: a pure deletion still carries a `diff --git` header for the prompt
# check to find.
work=$(make_fixture case_cx3d docs-extensionless-exec-deleted)
assert_tracked_mode 'case (cx3d) fixture' "$work" origin/main docs/reports/x-artifacts/ws0-results/ws0-readbw 100755
if [ -e "$work/docs/reports/x-artifacts/ws0-results/ws0-readbw" ]; then
  bad 'case (cx3d): the fixture still has the file on disk, so a filesystem stat could answer'
else
  ok 'case (cx3d): the path is absent from the working tree and from HEAD (only the BASE tree has it)'
fi
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_PROMPT='Review this diff:\ndiff --git a/docs/reports/x-artifacts/ws0-results/ws0-readbw b/docs/reports/x-artifacts/ws0-results/ws0-readbw\ndeleted file mode 100755'
run_wrapper "$work"
assert_verdict 'case (cx3d)' PASS 0
assert_says 'case (cx3d) deleting an extensionless executable is a reviewable code change' '^code-free: PASS$'
assert_says 'case (cx3d) the deletion is a code census path the reviewer received' '^prompt-content: PASS \(1/1 code census paths present\)$'
assert_enqueued 'case (cx3d)'

# ---------------------------------------------------------------------------------------------
# (cx3e)–(cx3j): THE RULE IS A DISJUNCTION OVER BOTH ENDPOINTS (#3229 round-13 blocker).
#
# (cx3a)–(cx3d) pin the mode SOURCE — the tree rather than `test -x` — and they were mutation-
# tested in that direction (swapping the tree read for a filesystem stat turned 4 assertions
# RED). What they could NOT see is the RANGE SEMANTICS: every one of those fixtures has the
# path at EXACTLY ONE endpoint (added ⇒ HEAD only; deleted ⇒ BASE only), so an implementation
# that consulted only ONE endpoint, or that stopped at the FIRST endpoint holding a record,
# passed all four. The round-12 implementation was the latter, and a path present at BOTH
# endpoints therefore never reached BASE: `100755`@BASE → `100644`@HEAD read NON-CODE, dropping
# the path from `census_code_paths` so `prompt-content: PASS (n/n)` made no claim about it.
#
# So the cases below cover the ENDPOINT COMBINATIONS, which is the property the previous round
# left unguarded:
#   (cx3e) present at BOTH, exec at BASE only  — e2e, the blocker itself
#   (cx3f) the same fixture with the path ABSENT from the prompt — the naming direction
#   (cx3g) present at BOTH, exec at HEAD only  — the mirror
#   (cx3h) the full matrix as a direct unit probe (both / HEAD-only / BASE-only / neither),
#          plus a glob-metacharacter name, plus the stderr cleanliness assert
#   (cx3i) MUTATION of the semantics: consult only HEAD, then only BASE
#   (cx3j) STRUCTURAL: the fold cannot exit early
# ---------------------------------------------------------------------------------------------
printf '== case (cx3e): an extensionless file EXECUTABLE AT THE BASE ONLY (chmod -x) is still CODE ==\n'
reset_stub
# THE ROUND-13 BLOCKER, at the summary level. `chmod -x` does not turn a script into prose, and
# the census subject is the RANGE — so a path executable at EITHER endpoint is a code path. The
# fixture is a PURE mode change, so the path is present at BOTH endpoints: the shape the four
# preceding cases cannot produce.
work=$(make_fixture case_cx3e docs-extensionless-exec-unset)
assert_tracked_mode 'case (cx3e) fixture' "$work" origin/main docs/reports/x-artifacts/ws0-results/ws0-readbw 100755
assert_tracked_mode 'case (cx3e) fixture' "$work" HEAD docs/reports/x-artifacts/ws0-results/ws0-readbw 100644
# The working-tree bit agrees with HEAD, so no filesystem stat could rescue this case either:
# every source of "is it executable" EXCEPT the BASE tree says no.
if [ -x "$work/docs/reports/x-artifacts/ws0-results/ws0-readbw" ]; then
  bad 'case (cx3e): the working-tree file is still executable, so the case does not isolate the BASE tree'
else
  ok 'case (cx3e): neither HEAD nor the working tree records the bit — only the BASE tree does'
fi
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_PROMPT='Review this diff:\ndiff --git a/docs/reports/x-artifacts/ws0-results/ws0-readbw b/docs/reports/x-artifacts/ws0-results/ws0-readbw\nold mode 100755\nnew mode 100644'
run_wrapper "$work"
assert_verdict 'case (cx3e)' PASS 0
assert_says 'case (cx3e) the census saw the mode change and the .md' '^census: 2 files, \+[0-9]+/-[0-9]+$'
assert_says 'case (cx3e) losing the exec bit is a reviewable code change, not prose' '^code-free: PASS$'
assert_says 'case (cx3e) the mode-changed path is the one code census path, and it arrived' '^prompt-content: PASS \(1/1 code census paths present\)$'
# Before the fix this run reported `code-free: FAIL` — the path was classified non-code, leaving
# the census with NO code path at all — so a `PASS` here can only come from counting it.
assert_lacks 'case (cx3e) the diff is never called code-free' '^code-free: FAIL'
assert_enqueued 'case (cx3e)'

printf '== case (cx3f): the SAME mode-changed path, ABSENT from the prompt, is a FAIL that NAMES it ==\n'
reset_stub
# The direction that makes (cx3e) mean something: the path must be a SUBJECT the guard can miss.
# Under the round-12 code it was not a subject at all, so its absence from the reviewer's prompt
# was unassertable — the defect was invisible from inside the guard's own output.
work=$(make_fixture case_cx3f docs-extensionless-exec-unset)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_PROMPT='Review this diff:\ndiff --git a/docs/reports/x-report.md b/docs/reports/x-report.md'
run_wrapper "$work"
assert_verdict 'case (cx3f)' FAIL 1
assert_says 'case (cx3f) the absent mode-changed executable is the one absent code path' '^prompt-content: FAIL \(1/1 code census paths absent from the prompt\)$'
assert_says 'case (cx3f) the absent path is NAMED' '^  docs/reports/x-artifacts/ws0-results/ws0-readbw$'
assert_lacks 'case (cx3f) never reports a PASS on a prompt missing the mode-changed path' '^prompt-content: PASS'

printf '== case (cx3g): the MIRROR — executable at HEAD only (chmod +x of an existing file) ==\n'
reset_stub
# A `chmod +x` of a file that already existed at the BASE: an executable ENTERING the range. Also
# present at both endpoints, and the direction a BASE-only consult would lose. Same expectation,
# because the rule is a disjunction and neither endpoint outranks the other.
work=$(make_fixture case_cx3g docs-extensionless-exec-set)
assert_tracked_mode 'case (cx3g) fixture' "$work" origin/main docs/reports/x-artifacts/ws0-results/ws0-readbw 100644
assert_tracked_mode 'case (cx3g) fixture' "$work" HEAD docs/reports/x-artifacts/ws0-results/ws0-readbw 100755
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_PROMPT='Review this diff:\ndiff --git a/docs/reports/x-artifacts/ws0-results/ws0-readbw b/docs/reports/x-artifacts/ws0-results/ws0-readbw\nold mode 100644\nnew mode 100755'
run_wrapper "$work"
assert_verdict 'case (cx3g)' PASS 0
assert_says 'case (cx3g) gaining the exec bit is a reviewable code change' '^code-free: PASS$'
assert_says 'case (cx3g) the newly-executable path is the one code census path' '^prompt-content: PASS \(1/1 code census paths present\)$'
assert_enqueued 'case (cx3g)'

printf '== case (cx3h): the ENDPOINT-COMBINATION MATRIX (direct unit probe) ==\n'
# Probed DIRECTLY, through the real function in the real file, because ONE repository can carry
# every combination at once — which is what makes the table a table rather than six unrelated
# fixtures whose expectations could drift apart. `absent-everywhere` is only reachable this way:
# a real census path exists at an endpoint by construction, so the unmeasurable case has no
# fixture that could produce it end-to-end.
mode_matrix_repo() { # mode_matrix_repo <dir> -> prints the BASE sha
  local w="$1"
  mkdir -p "$w"
  git init -q -b main "$w"
  git_q "$w" config user.email guard@example.invalid
  git_q "$w" config user.name 'guard test'
  local p
  # BASE tree. `chmod` AND `update-index --chmod` for the same reason as every other
  # mode-bearing fixture here: neither alone is trustworthy.
  for p in both-exec base-only-exec base-exec-head-plain 'glob[x]*?-exec'; do
    printf '#!/bin/sh\nexit 0\n' >"$w/$p"; chmod 755 "$w/$p"
  done
  for p in both-plain head-exec-base-plain; do
    printf 'plain\n' >"$w/$p"; chmod 644 "$w/$p"
  done
  printf 'base\n' >"$w/README.md"
  git_q "$w" add -A
  git_q "$w" update-index --chmod=+x both-exec --chmod=+x base-only-exec \
    --chmod=+x base-exec-head-plain --chmod=+x 'glob[x]*?-exec'
  git_q "$w" update-index --chmod=-x both-plain --chmod=-x head-exec-base-plain
  git_q "$w" commit -q -m base
  local base; base=$(git -C "$w" rev-parse HEAD)
  # HEAD tree: one transition per path. `rm -f` because the index/worktree mode agreement above
  # is what made a plain `git rm` refuse in the deleted fixture.
  git_q "$w" rm -q -f base-only-exec
  chmod 644 "$w/base-exec-head-plain"
  chmod 755 "$w/head-exec-base-plain"
  printf '#!/bin/sh\nexit 0\n' >"$w/head-only-exec"; chmod 755 "$w/head-only-exec"
  printf 'plain\n' >"$w/head-only-plain"; chmod 644 "$w/head-only-plain"
  git_q "$w" add -A
  git_q "$w" update-index --chmod=-x base-exec-head-plain --chmod=+x head-exec-base-plain \
    --chmod=+x head-only-exec --chmod=-x head-only-plain
  git_q "$w" commit -q -m head
  printf '%s' "$base"
}
# The probe prints one `<path><TAB>CODE|NON-CODE|UNMEASURABLE` line per subject. It sources the
# REAL oracles file, so a future change to the function is measured, not a copy of it.
#
# THREE OUTCOME WORDS, NOT TWO (#3229 round-14 blocker). An `if roborev_path_exec_state` probe
# would be a BOOLEAN probe over a tri-valued function: `if` collapses 1 and 2 into "false", so
# UNMEASURABLE would print `NON-CODE` and the probe would report the very defect under test as
# the expected answer. It therefore reads the exit STATUS and prints a distinct word per state —
# the probe itself must be able to express "could not measure", or it cannot measure it.
cx3h_probe="$tmp/cx3h-probe.sh"
cat >"$cx3h_probe" <<'CX3H'
set -uo pipefail
# shellcheck disable=SC1090
. "$1"          # the real oracles file
REPO="$2"
# The range's LEFT endpoint is the MERGE-BASE (`RANGE_BASE_SHA`, #3392), not the base ref's
# tip: `roborev_range_endpoint_refs` folds over HEAD and that. In this linear fixture the two
# would coincide anyway; the name is what the oracles actually read.
RANGE_BASE_SHA="$3"
for p in both-exec both-plain head-only-exec head-only-plain base-only-exec \
         base-exec-head-plain head-exec-base-plain 'glob[x]*?-exec' absent-everywhere; do
  st=0
  roborev_path_exec_state "$p" || st=$?
  case "$st" in
    0) printf '%s\tCODE\n' "$p" ;;
    1) printf '%s\tNON-CODE\n' "$p" ;;
    *) printf '%s\tUNMEASURABLE\t%s\n' "$p" "${ROBOREV_EXEC_UNMEASURABLE_REFS[*]:-<none>}" ;;
  esac
done
CX3H
# `<path> <expected>`; the comment on each line is the endpoint combination it stands for.
CX3H_MATRIX=(
  'both-exec CODE'                 # present at both, exec at both
  'both-plain NON-CODE'            # present at both, exec at neither  -> the only non-code both-case
  'head-only-exec CODE'            # added executable          (HEAD only)
  'head-only-plain NON-CODE'       # added non-executable      (HEAD only)
  'base-only-exec CODE'            # deleted executable        (BASE only)
  'base-exec-head-plain CODE'      # chmod -x  — THE ROUND-13 BLOCKER
  'head-exec-base-plain CODE'      # chmod +x  — the mirror
  'glob[x]*?-exec CODE'            # a name full of glob metacharacters, exec at both
  # Present at NEITHER endpoint. `ls-tree` SUCCEEDED at both and reported no record, so this is a
  # real MEASUREMENT of absence and NOT-EXEC is the right answer — the distinction the tri-valued
  # leaf exists to keep (#3229 round-14). Pinned here so the fix cannot be "fail closed on
  # everything", which would break the added/deleted cases above.
  'absent-everywhere NON-CODE'
)
cx3h_work="$tmp/case_cx3h/work"
cx3h_base=$(mode_matrix_repo "$cx3h_work")
cx3h_out="$tmp/cx3h-out.txt"
cx3h_err="$tmp/cx3h-err.txt"
# The fixture must really hold the combinations the table claims, or the table measures nothing.
assert_tracked_mode 'case (cx3h) fixture' "$cx3h_work" "$cx3h_base" base-exec-head-plain 100755
assert_tracked_mode 'case (cx3h) fixture' "$cx3h_work" HEAD base-exec-head-plain 100644
assert_tracked_mode 'case (cx3h) fixture' "$cx3h_work" "$cx3h_base" head-exec-base-plain 100644
assert_tracked_mode 'case (cx3h) fixture' "$cx3h_work" HEAD head-exec-base-plain 100755
assert_tracked_mode 'case (cx3h) fixture' "$cx3h_work" "$cx3h_base" base-only-exec 100755
if git -C "$cx3h_work" ls-tree HEAD -- ':(literal)base-only-exec' | grep -q .; then
  bad 'case (cx3h): base-only-exec still exists at HEAD, so the BASE-only combination is not reproduced'
else
  ok 'case (cx3h): base-only-exec is absent from HEAD (the BASE-only combination is reproduced)'
fi
# `run_probe` is deliberately a plain `bash`: the point is the FUNCTION, and the wrapper's
# summary block cannot express `absent-everywhere` at all.
if bash "$cx3h_probe" "$ORACLES_SRC" "$cx3h_work" "$cx3h_base" >"$cx3h_out" 2>"$cx3h_err"; then
  for _row in "${CX3H_MATRIX[@]}"; do
    _mpath="${_row% *}"; _mwant="${_row##* }"
    if grep -Fxq "$(printf '%s\t%s' "$_mpath" "$_mwant")" "$cx3h_out"; then
      ok "case (cx3h): $_mpath => $_mwant"
    else
      bad "case (cx3h): $_mpath => want $_mwant, got '$(grep -F "$_mpath" "$cx3h_out" | head -1)'"
    fi
  done
else
  bad "case (cx3h): the matrix probe did not run: $(cat "$cx3h_err")"
fi
# THE NUL WARNING (#3229 round-13, folded in). `git ls-tree -z` piped through `$(...)` made bash
# emit `warning: command substitution: ignored null byte in input` on EVERY call — harmless for a
# single record (the path is last, so only the terminating NUL is lost) but per-call stderr noise
# that can MASK a real warning. Nine calls above, so a per-call warning cannot hide here.
if [ -s "$cx3h_err" ]; then
  bad "case (cx3h): the classifier wrote to stderr: $(head -2 "$cx3h_err")"
else
  ok 'case (cx3h): nine classifications produced NO stderr (the ignored-null-byte warning is gone)'
fi

printf '== case (cx3i): MUTATION — consulting only ONE endpoint must go RED ==\n'
# The previous round's mutation testing swapped the mode SOURCE (tree ⇒ `test -x`) and turned 4
# assertions red, which is why it looked sufficient. It never mutated the RANGE SEMANTICS, and
# that is the axis the blocker lived on. So both single-endpoint mutants are run here, and each
# must break the combination only the OTHER endpoint can answer — which also proves the two
# endpoints are not redundant.
#
#   HEAD-only consult ⇒ `base-only-exec` (a deleted executable) must go NON-CODE
#   BASE-only consult ⇒ `head-only-exec` (an added executable)  must go NON-CODE
#
# Both mutants also flip a mode-change case, and that is asserted too: it is the same defect the
# blocker reported, reachable from either single-endpoint direction.
cx3i_mut="$tmp/cx3i-oracles.sh"
for _mut in head base; do
  cp "$ORACLES_SRC" "$cx3i_mut"
  if [ "$_mut" = head ]; then
    _mut_from='HEAD "${RANGE_BASE_SHA:-}"'; _mut_to='HEAD'
    _mut_lost=base-only-exec; _mut_lost2=base-exec-head-plain; _mut_kept=head-only-exec
  else
    _mut_from='HEAD "${RANGE_BASE_SHA:-}"'; _mut_to='"${RANGE_BASE_SHA:-}"'
    _mut_lost=head-only-exec; _mut_lost2=head-exec-base-plain; _mut_kept=base-only-exec
  fi
  # Patch the ENDPOINT PRODUCER, which is the single place the range is named — a mutant that
  # could not be expressed as a one-line edit there would itself be evidence the shape is wrong.
  python3 - "$cx3i_mut" "$_mut_from" "$_mut_to" <<'CX3I' || bad "case (cx3i/$_mut): could not patch the endpoint producer"
import sys
p, frm, to = sys.argv[1], sys.argv[2], sys.argv[3]
s = open(p).read()
old = 'for ref in %s; do' % frm
if s.count(old) != 1:
    sys.exit('expected exactly one endpoint-producer loop, found %d' % s.count(old))
open(p, 'w').write(s.replace(old, 'for ref in %s; do' % to, 1))
CX3I
  if grep -qF "for ref in $_mut_to; do" "$cx3i_mut" && ! grep -qF "for ref in $_mut_from; do" "$cx3i_mut"; then
    ok "case (cx3i/$_mut): the single-endpoint mutant was really applied to the copy"
  else
    bad "case (cx3i/$_mut): the mutant was NOT applied, so nothing was mutation-tested (a green here is a probe failing toward success)"
  fi
  if bash "$cx3h_probe" "$cx3i_mut" "$cx3h_work" "$cx3h_base" >"$tmp/cx3i-$_mut.txt" 2>/dev/null; then
    for _lost in "$_mut_lost" "$_mut_lost2"; do
      if grep -Fxq "$(printf '%s\tNON-CODE' "$_lost")" "$tmp/cx3i-$_mut.txt"; then
        ok "case (cx3i/$_mut): consulting only $_mut makes $_lost read NON-CODE — the assert is load-bearing"
      else
        bad "case (cx3i/$_mut): $_lost still read CODE under a $_mut-only consult, so the either-endpoint asserts do not detect a skipped endpoint"
      fi
    done
    # The mutant must NOT be uniformly broken: the endpoint it DOES consult still answers, so the
    # two REDs above are the semantics and not a probe that stopped working.
    if grep -Fxq "$(printf '%s\tCODE' "$_mut_kept")" "$tmp/cx3i-$_mut.txt"; then
      ok "case (cx3i/$_mut): the mutant still classifies $_mut_kept CODE (it lost one endpoint, not the whole function)"
    else
      bad "case (cx3i/$_mut): the mutant lost $_mut_kept too, so it is broken rather than single-endpoint"
    fi
  else
    bad "case (cx3i/$_mut): the mutated probe did not run at all"
  fi
done
# RESTORED GREEN: the unmutated file, re-measured after the mutants, so a mutation that leaked
# into the real file (a stray in-place edit, a wrong path) cannot pass unnoticed.
if bash "$cx3h_probe" "$ORACLES_SRC" "$cx3h_work" "$cx3h_base" >"$tmp/cx3i-restored.txt" 2>/dev/null &&
  diff -q "$cx3h_out" "$tmp/cx3i-restored.txt" >/dev/null; then
  ok 'case (cx3i): the UNMUTATED oracles file still produces the full green matrix (no mutant leaked into it)'
else
  bad 'case (cx3i): the unmutated matrix changed after the mutation run — a mutant leaked into the real file'
fi

printf '== case (cx3j): STRUCTURAL — the endpoint fold cannot exit early ==\n'
# The behavioural cases above pin the RULE. This pins the SHAPE that makes the rule hold BY
# CONSTRUCTION, because the shape is the actual remedy: this is the third round on this PR where
# a fix restored a narrower instance of the class it closed, and each time the code was correct
# for the cases someone had thought of. A `return`/`break`/`continue` inside the fold is what
# "skip an endpoint" LOOKS like, so its absence is asserted directly — a future edit that
# reintroduces one FAILs here even if it happens to be correct for every fixture above.
#
# SHAPE-AGNOSTIC ON PURPOSE. A check keyed to the CURRENT loop spelling (`while … read -r ref`)
# would go quiet the moment someone rewrote the fold — including back into the `for`-loop shape
# the blocker came from — so it asserts the INVARIANT instead: `roborev_path_exec_state`
# contains NO `break`/`continue` at all, and EXACTLY ONE `return`, which is its LAST statement.
# The round-12 code had three returns, two of them inside the loop; any reintroduced early exit
# adds a return, a break or a continue, whatever the loop is written with.
#
# `cx3j_shape <file>` prints one word: `OK`, `NOT-FOUND`, or a named violation. Comment lines are
# stripped first — the body deliberately DOCUMENTS that it has no early exit, and a check that
# read its own prose fired on the word "exit" in that comment (measured).
cx3j_shape() { # cx3j_shape <file> [funcname]
  local file="$1" fn="${2:-roborev_path_exec_state}" body last nret
  body=$(awk -v fn="^$fn\\\\(\\\\) \\\\{" '
    $0 ~ fn { inf = 1; next }
    inf && /^\}/ { exit }
    inf { print }
  ' "$file" | grep -vE '^[[:space:]]*#' | grep -vE '^[[:space:]]*$')
  if [ -z "$body" ]; then printf 'NOT-FOUND\n'; return 0; fi
  if printf '%s\n' "$body" | grep -qE '(^|[^[:alnum:]_])(break|continue)([^[:alnum:]_]|$)'; then
    printf 'HAS-BREAK-OR-CONTINUE\n'; return 0
  fi
  nret=$(printf '%s\n' "$body" | grep -cE '(^|[^[:alnum:]_])return([^[:alnum:]_]|$)' || true)
  if [ "$nret" -ne 1 ]; then printf 'RETURNS=%s\n' "$nret"; return 0; fi
  last=$(printf '%s\n' "$body" | tail -1)
  case "$last" in
    *return*) printf 'OK\n' ;;
    *) printf 'RETURN-NOT-LAST\n' ;;
  esac
}
# BOTH DIRECTIONS. The check is only worth its line if it FIRES, so it is run against three
# mutants first: a `return` injected into the fold, a `break` injected into the fold, and the
# ACTUAL round-12 shape (an ordered `for` loop that returns on the first record). If any of them
# read `OK`, the assert below proves nothing.
cx3j_mut="$tmp/cx3j-mutant.sh"
cx3j_inject() { # cx3j_inject <stmt> — copy the oracles with <stmt> added inside the fold
  python3 - "$ORACLES_SRC" "$cx3j_mut" "$1" <<'CX3JI'
import sys
src, dst, stmt = sys.argv[1], sys.argv[2], sys.argv[3]
s = open(src).read()
anchor = '    _roborev_mode_exec_state_at "$ref" "$path" || st=$?\n'
if s.count(anchor) != 1:
    sys.exit('expected exactly one fold body, found %d' % s.count(anchor))
open(dst, 'w').write(s.replace(anchor, anchor.rstrip('\n') + '\n    ' + stmt + '\n', 1))
CX3JI
}
for _inj in 'return 0' 'break'; do
  if cx3j_inject "$_inj"; then
    _got=$(cx3j_shape "$cx3j_mut")
    if [ "$_got" != OK ]; then
      ok "case (cx3j control): a '$_inj' injected into the fold is caught ($_got)"
    else
      bad "case (cx3j control): a '$_inj' injected into the fold read OK — the structural assert cannot see an early exit"
    fi
  else
    bad "case (cx3j control): could not inject '$_inj' into the fold, so the assert was never controlled"
  fi
done
# The round-12 shape itself, verbatim from the commit this round fixes.
{
  printf 'roborev_path_exec_state() {\n'
  printf '  local path="$1" ref record mode\n'
  printf '  for ref in HEAD "${BASE_SHA:-}"; do\n'
  printf '    [ -n "$ref" ] || continue\n'
  printf '    record=$(git -C "$REPO" ls-tree -z "$ref" -- ":(literal)$path" 2>/dev/null) || continue\n'
  printf '    [ -n "$record" ] || continue\n'
  printf '    mode="${record%%%% *}"\n'
  printf '    [ "$mode" = 100755 ] && return 0\n'
  printf '    return 1\n'
  printf '  done\n'
  printf '  return 1\n'
  printf '}\n'
} >"$tmp/cx3j-round12.sh"
_got=$(cx3j_shape "$tmp/cx3j-round12.sh")
if [ "$_got" != OK ]; then
  ok "case (cx3j control): the ROUND-12 shape this round replaced is caught ($_got)"
else
  bad 'case (cx3j control): the round-12 shape read OK — the structural assert would not have caught the reported blocker'
fi
# A function that is absent must read NOT-FOUND rather than OK, or a rename would pass vacuously.
_got=$(cx3j_shape "$ORACLES_SRC" roborev_no_such_function_exists)
if [ "$_got" = NOT-FOUND ]; then
  ok 'case (cx3j control): an ABSENT function reads NOT-FOUND, never OK (a rename cannot pass vacuously)'
else
  bad "case (cx3j control): an absent function read '$_got' — the shape check can pass on nothing"
fi
# THE ASSERT ITSELF.
cx3j_got=$(cx3j_shape "$ORACLES_SRC")
if [ "$cx3j_got" = OK ]; then
  ok 'case (cx3j): roborev_path_exec_state has no break/continue and exactly ONE return, last — no endpoint can be skipped'
else
  bad "case (cx3j): roborev_path_exec_state's shape reads '$cx3j_got' — an endpoint can be skipped again"
fi
# The per-endpoint predicate must stay RANGE-BLIND: if it learns about RANGE_BASE_SHA (or the
# base ref's tip, BASE_TIP_SHA) it can express a precedence again, which is the ambiguity the
# split exists to remove.
cx3j_pred=$(awk '
  /^_roborev_mode_exec_state_at\(\) \{/ { inf = 1 }
  inf { print }
  inf && /^\}/ { exit }
' "$ORACLES_SRC")
if [ -z "$cx3j_pred" ]; then
  bad 'case (cx3j): _roborev_mode_exec_state_at was not found, so its range-blindness was not checked'
else
  if ! printf '%s\n' "$cx3j_pred" | grep -vE '^[[:space:]]*#' | grep -qE 'RANGE_BASE_SHA|BASE_TIP_SHA|\bHEAD\b'; then
    ok 'case (cx3j): the per-endpoint predicate names no endpoint (range-blind, so it cannot encode an ordering)'
  else
    bad 'case (cx3j): _roborev_mode_exec_state_at references a specific endpoint — it can encode a precedence again'
  fi
fi

printf '== case (cx3k): the TRI-VALUED LEAF — a FAILED measurement is not a measured NO ==\n'
# THE ROUND-14 BLOCKER, and the NINTH instance on this PR of "could not measure" rendered as
# "nothing wrong". The round-13 leaf was `record=$(git ls-tree …) || return 1`, so a FAILED lookup
# returned the SAME value as a measured non-executable and a genuinely executable file classified
# as PROSE on an infra fault — dropped from `census_code_paths`, asserted about by nothing, green
# summary. The level-shift is the lesson: round 13 made the FOLD order-independent by construction
# while leaving the LEAF two-valued, so it proved the right property ONE LEVEL TOO HIGH. A boolean
# cannot express "I could not tell", so it must collapse uncertainty onto the permissive side.
#
# The probe is `cx3h_probe`, reused unchanged: it already prints THREE distinct outcome words, so
# it can express the state under test. Fault injection is `REPO` pointed at a directory that is
# NOT a git repository — the reviewer's own reproduction, and the honest shape of the condition
# (every `ls-tree` fails; no other git call is involved in this function).
cx3k_run() { # cx3k_run <oracles> <repo> <base> -> writes stdout to $tmp/cx3k-<tag>.txt
  bash "$cx3h_probe" "$1" "$2" "$3" >"$tmp/cx3k-$4.txt" 2>"$tmp/cx3k-$4.err"
}
cx3k_want() { # cx3k_want <tag> <label> <path> <expected-word>
  if grep -q "^$(printf '%s\t%s' "$3" "$4")" "$tmp/cx3k-$1.txt"; then
    ok "case (cx3k/$1): $3 => $4 — $2"
  else
    bad "case (cx3k/$1): $3 => want $4, got '$(grep -F "$3" "$tmp/cx3k-$1.txt" | head -1)' — $2"
  fi
}
cx3k_notrepo="$tmp/cx3k-not-a-git-repo"
mkdir -p "$cx3k_notrepo"

# --- (1) and (2): the two REGRESSION/MINIMALITY controls, on the valid repo. These are the same
# subjects the cx3h matrix covers, re-asserted here so the four cx3k rows are read as one table:
# a "fix" that returned CODE unconditionally — or UNMEASURABLE unconditionally — fails here.
cx3k_run "$ORACLES_SRC" "$cx3h_work" "$cx3h_base" valid
cx3k_want valid 'a valid repo still measures an executable'            both-exec   CODE
cx3k_want valid 'a valid repo still measures NON-executable prose'     both-plain  NON-CODE
# --- (3): `ls-tree` SUCCEEDED and returned NO RECORD. A REAL measurement of absence, and it must
# stay on the measured side or the added/deleted endpoint combinations (cx3d, cx3h) break.
cx3k_want valid 'ls-tree SUCCEEDED with no record is MEASURED-absent, not unmeasurable' \
  absent-everywhere NON-CODE

# --- (4): `ls-tree` FAILS AT BOTH ENDPOINTS for a genuinely EXECUTABLE path. `both-exec` is
# recorded 100755 at both endpoints of the real fixture (asserted above by cx3h), so the ONLY
# variable is that the lookup cannot run.
cx3k_run "$ORACLES_SRC" "$cx3k_notrepo" "$cx3h_base" both-unmeasurable
cx3k_want both-unmeasurable 'a genuinely executable path with BOTH lookups failing is UNMEASURABLE, never a quiet NON-CODE' \
  both-exec UNMEASURABLE
if grep -q '^both-exec	UNMEASURABLE.*not a git repository\|^both-exec	UNMEASURABLE.*cannot change to\|^both-exec	UNMEASURABLE.*fatal' "$tmp/cx3k-both-unmeasurable.txt"; then
  ok "case (cx3k/both-unmeasurable): the unmeasurable refs carry git's OWN message, so the operator is told why"
else
  bad "case (cx3k/both-unmeasurable): no git message was recorded: '$(grep -F both-exec "$tmp/cx3k-both-unmeasurable.txt" | head -1)'"
fi
# BOTH DIRECTIONS on the injector itself: it must NOT make everything unmeasurable by accident of
# how the probe is invoked — `both-plain` is unmeasurable here too (correctly, the whole repo is
# unreadable), so the discriminating control is the VALID run above reading NON-CODE for it.
cx3k_want both-unmeasurable 'and a non-executable path is unmeasurable too — the fault is the REPO, not the path' \
  both-plain UNMEASURABLE

# --- (5): `ls-tree` fails at ONE endpoint only. THE LATTICE IS THE ANSWER, and it is pinned in
# BOTH sub-directions, because a single row could be satisfied by either a fail-open or a
# fail-closed-on-everything implementation:
#     exec@HEAD    + unmeasurable@BASE  => CODE          (EXEC dominates UNMEASURABLE)
#     NOT-exec@HEAD + unmeasurable@BASE => UNMEASURABLE  (UNMEASURABLE dominates NOT-EXEC)
# EXEC dominating is SOUND, not lenient: the rule is a DISJUNCTION over the endpoints, so positive
# evidence at one endpoint settles it — whatever the failed endpoint would have said could only be
# another "yes", and no "yes" un-satisfies a disjunction. NOT-EXEC does NOT dominate, because
# "executable at NEITHER endpoint" is a claim about EVERY endpoint and one unmeasured endpoint
# leaves it unfounded. So non-code stays reachable ONLY from a positive measurement everywhere.
cx3k_run "$ORACLES_SRC" "$cx3h_work" 0000000000000000000000000000000000000000 one-unmeasurable
cx3k_want one-unmeasurable 'exec at the measurable endpoint DOMINATES an unmeasurable one (a disjunction cannot be un-satisfied)' \
  both-exec CODE
cx3k_want one-unmeasurable 'NON-exec at the measurable endpoint yields UNMEASURABLE — "exec at NEITHER" is a claim about EVERY endpoint' \
  both-plain UNMEASURABLE
for _tag in valid both-unmeasurable one-unmeasurable; do
  if [ -s "$tmp/cx3k-$_tag.err" ]; then
    bad "case (cx3k/$_tag): the classifier wrote to stderr: $(head -2 "$tmp/cx3k-$_tag.err")"
  else
    ok "case (cx3k/$_tag): the classifier produced NO stderr (git's message is captured, not leaked)"
  fi
done

printf '== case (cx3k-mut): MUTATION — the leaf reverted to TWO-VALUED must go RED ==\n'
# The mutation that matters is not "break the function", it is "restore the exact prior shape".
# `|| return 1` in place of the tri-valued failure branch is the round-13 code verbatim, and under
# it every UNMEASURABLE row above reads NON-CODE — a genuinely executable file called prose because
# the lookup failed. If this mutant did NOT flip those rows, the cx3k asserts would be measuring
# nothing.
cx3k_mut="$tmp/cx3k-two-valued-oracles.sh"
python3 - "$ORACLES_SRC" "$cx3k_mut" <<'CX3KM' || bad 'case (cx3k-mut): could not build the two-valued mutant'
import sys
src, dst = sys.argv[1], sys.argv[2]
s = open(src, encoding='utf-8').read()
# The tri-valued failure branch, replaced by the round-13 two-valued spelling: a FAILED lookup
# returns the SAME value as a measured non-executable.
old = '''  errfile=$(mktemp "${TMPDIR:-/tmp}/roborev-exec-state.XXXXXX") || return 2
  rc=0
  record=$(git -C "$REPO" ls-tree "$ref" -- ":(literal)$path" 2>"$errfile") || rc=$?
  if [ "$rc" -ne 0 ]; then
'''
if s.count(old) != 1:
    sys.exit('expected exactly one tri-valued failure branch, found %d' % s.count(old))
new = '''  errfile=$(mktemp "${TMPDIR:-/tmp}/roborev-exec-state.XXXXXX") || return 1
  rc=0
  record=$(git -C "$REPO" ls-tree "$ref" -- ":(literal)$path" 2>"$errfile") || rc=$?
  if [ "$rc" -ne 0 ]; then
    rm -f "$errfile"
    return 1
  fi
  if false; then
'''
open(dst, 'w', encoding='utf-8').write(s.replace(old, new, 1))
CX3KM
if grep -qF 'if false; then' "$cx3k_mut" && ! grep -qF 'if false; then' "$ORACLES_SRC"; then
  ok 'case (cx3k-mut): the two-valued mutant was really applied to the COPY only'
else
  bad 'case (cx3k-mut): the mutant was NOT applied (or leaked into the real file), so nothing was mutation-tested — a green here would be a probe failing toward success'
fi
cx3k_run "$cx3k_mut" "$cx3h_work" "$cx3h_base" mut-valid
cx3k_run "$cx3k_mut" "$cx3k_notrepo" "$cx3h_base" mut-unmeasurable
# THE RED: the mutant calls a genuinely executable, genuinely unmeasurable path PROSE.
cx3k_want mut-unmeasurable 'the two-valued leaf collapses UNMEASURABLE onto the permissive side' \
  both-exec NON-CODE
# NOT UNIFORMLY BROKEN: on the valid repo the mutant still classifies correctly, so the row above
# is the tri-value distinction and not a mutant that stopped working.
cx3k_want mut-valid 'the mutant still measures a valid repo correctly (it lost the third state, not the function)' \
  both-exec CODE
# RESTORED GREEN, re-measured after the mutant, so a stray in-place edit cannot pass unnoticed.
cx3k_run "$ORACLES_SRC" "$cx3k_notrepo" "$cx3h_base" restored
if diff -q "$tmp/cx3k-both-unmeasurable.txt" "$tmp/cx3k-restored.txt" >/dev/null; then
  ok 'case (cx3k-mut): the UNMUTATED oracles file still reports UNMEASURABLE (no mutant leaked into it)'
else
  bad 'case (cx3k-mut): the unmutated result changed after the mutation run — a mutant leaked into the real file'
fi

printf '== case (cx3l): END-TO-END — an unmeasurable mode FAILS the run CLOSED and NAMES the path ==\n'
# The unit rows above pin the lattice; this pins the CONSEQUENCE, through the wrapper's own summary
# block, which is the only surface a consumer reads. An unmeasurable classification must not be
# spendable as prose (`code-free: FAIL`, which would read "docs-only, nothing to review") and must
# not be spendable as a pass — it is a THIRD outcome and it fails closed on `census-check:`.
#
# FAULT INJECTION: a `git` shim, first on PATH, that forwards every subcommand to the real binary
# EXCEPT `ls-tree`, which fails. Scoped and honest — `ls-tree` has exactly ONE caller in the whole
# wrapper (the leaf), so nothing else in the run is perturbed, and the wrapper's own rev-parse /
# ls-remote / diff still work. Pointing `--repo` at a non-repo (the unit injector) cannot be used
# here: the wrapper would fail at push-assert long before the census.
cx3l_bin="$tmp/cx3l-bin"
mkdir -p "$cx3l_bin"
cx3l_real_git=$(command -v git)
cat >"$cx3l_bin/git" <<CX3L
#!/usr/bin/env bash
for a in "\$@"; do
  if [ "\$a" = ls-tree ]; then
    printf 'fatal: injected ls-tree failure (cx3l fault injection)\n' >&2
    exit 128
  fi
done
exec "$cx3l_real_git" "\$@"
CX3L
chmod +x "$cx3l_bin/git"
# THE CONTROL FIRST, AND IT IS NOT OPTIONAL. An assert that the shimmed run FAILs is satisfied by a
# shim that broke the run for some other reason, which is a probe failing in the direction that
# looks like success. So the SAME fixture is shown to reach PASS with the shim absent — one
# variable, `ls-tree`.
reset_stub
work=$(make_fixture case_cx3l docs-extensionless-exec)
assert_tracked_mode 'case (cx3l) fixture' "$work" HEAD docs/reports/x-artifacts/ws0-results/ws0-readbw 100755
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_PROMPT='Review this diff:\ndiff --git a/docs/reports/x-artifacts/ws0-results/ws0-readbw b/docs/reports/x-artifacts/ws0-results/ws0-readbw\ndiff --git a/docs/reports/x-artifacts/harness/plain.sh b/docs/reports/x-artifacts/harness/plain.sh'
run_wrapper "$work"
assert_verdict 'case (cx3l control) the SAME fixture without the shim reaches PASS' PASS 0
assert_says 'case (cx3l control) census-check PASSes when ls-tree works' '^census-check: PASS$'
assert_enqueued 'case (cx3l control)'
# NOW the one variable.
reset_stub
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_PROMPT='Review this diff:\ndiff --git a/docs/reports/x-artifacts/ws0-results/ws0-readbw b/docs/reports/x-artifacts/ws0-results/ws0-readbw\ndiff --git a/docs/reports/x-artifacts/harness/plain.sh b/docs/reports/x-artifacts/harness/plain.sh'
WRAPPER_PATH_PREFIX="$cx3l_bin"
run_wrapper "$work"
WRAPPER_PATH_PREFIX=""
assert_verdict 'case (cx3l)' FAIL 1
# The two EXTENSIONLESS paths (`ws0-readbw` 100755, `NOTICE` 100644) are the only ones whose
# classification consults the mode, so exactly 2 of the 4 census files are unmeasurable. The
# count is the assertion: a fix that failed closed on EVERY path would read 4 of 4.
assert_says 'case (cx3l) census-check FAILs closed and counts the unmeasurable paths' \
  '^census-check: FAIL \(recorded mode unmeasurable for 2 of 4 census paths\)$'
assert_says 'case (cx3l) the genuinely EXECUTABLE path is NAMED, with the endpoint ref and git.s own message' \
  '^  docs/reports/x-artifacts/ws0-results/ws0-readbw @ .*injected ls-tree failure'
assert_says 'case (cx3l) the range endpoints are named as the refs that could not be measured' \
  '^  docs/reports/x-artifacts/ws0-results/ws0-readbw @ HEAD: .*[0-9a-f]{40}: '
assert_says 'case (cx3l) the wording says WE COULD NOT TELL, never NOTHING WAS WRONG' \
  "an unmeasurable mode is 'we cannot tell', never 'there is nothing wrong'"
# THE MISATTRIBUTION DIRECTIONS, both barred. Not a pass, and not "docs-only" either.
assert_lacks 'case (cx3l) never a PASS' '^RESULT: PASS'
assert_lacks 'case (cx3l) never census-check PASS' '^census-check: PASS'
assert_lacks 'case (cx3l) NEVER spent as a code-free/docs-only diff' '^code-free: FAIL'
assert_lacks 'case (cx3l) and never NOTHING-TO-REVIEW' '^RESULT: NOTHING-TO-REVIEW'
assert_never_enqueued 'case (cx3l)'
assert_one_result_line 'case (cx3l)'

printf '== case (cx6): a census path with SPACES and a literal quote compares correctly ==\n'
reset_stub
# NUL-safety. `git diff --numstat` C-QUOTES this path while `-z` output does not, so an
# un-normalised comparison would report a SURVIVING path as swallowed (a false FAIL) —
# the direction that gets a guard bypassed.
work=$(make_fixture case_cx6 docs-odd-name)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_PROMPT='Review this diff:\ndiff --git a/main.rs b/main.rs\ndiff --git a/docs/reports/x-artifacts/harness/odd "q" name.sh b/docs/reports/x-artifacts/harness/odd "q" name.sh'
run_wrapper "$work"
# ASSERT THE VERDICT, not just one key (#3229 round 3, blocker F3). This case used to
# assert a single key and therefore reported `ok` twice while the SAME hostile path
# false-FAILed `prompt-content:` (MEASURED: one key reporting `PASS (2/2 …)` beside
# `prompt-content: FAIL (1/2 absent)`, `RESULT: FAIL`) — a case that passes while the
# behaviour it names is broken is worse than no case at all.
assert_verdict 'case (cx6)' PASS 0
assert_says 'case (cx6) prompt-content compares the quoted census path against the prompt NORMALISED' '^prompt-content: PASS \(2/2 code census paths present\)$'
assert_lacks 'case (cx6) prompt-content does not false-FAIL on a quoting artefact' '^prompt-content: FAIL'

printf '== case (cx6k): the same path in the header shape git REALLY emits for a quote ==\n'
reset_stub
# (cx6) hands the wrapper an UNQUOTED header carrying a literal `"` — a producer that is
# not git. git itself C-QUOTES a quote-bearing path and ESCAPES the inner quotes:
# `diff --git "a/…odd \"q\" name.sh" "b/…"`. Both readings must count as present, so the
# escaped-quote round trip is pinned separately rather than assumed to follow from (cx6).
work=$(make_fixture case_cx6k docs-odd-name)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_PROMPT='Review this diff:\ndiff --git a/main.rs b/main.rs\ndiff --git "a/docs/reports/x-artifacts/harness/odd \\"q\\" name.sh" "b/docs/reports/x-artifacts/harness/odd \\"q\\" name.sh"'
run_wrapper "$work"
assert_verdict 'case (cx6k)' PASS 0
assert_says 'case (cx6k) the escaped-quote header shape counts as present' '^prompt-content: PASS \(2/2 code census paths present\)$'

printf '== case (cx6c): a CODE path under a SPACE-bearing directory does not false-FAIL prompt-content ==\n'
reset_stub
# `docs/storage engine/` is a real tracked directory in this repo (40 space-bearing paths
# under docs/), and this change promotes docs/-scoped executables to CODE census paths.
# git does not quote a space-only path, so the header is `diff --git a/a b.sh b/a b.sh`,
# which `a/[^ ]+ b/[^ ]+` cannot split — matched instead by probing the LITERAL header.
work=$(make_fixture case_cx6c docs-space-dir)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_PROMPT='Review this diff:\ndiff --git a/main.rs b/main.rs\ndiff --git a/docs/storage engine/probe.sh b/docs/storage engine/probe.sh'
run_wrapper "$work"
assert_verdict 'case (cx6c)' PASS 0
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
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_PROMPT='Review this diff:\ndiff --git a/main.rs b/main.rs\ndiff --git "a/docs/reports/x-artifacts/\\303\\251.sh" "b/docs/reports/x-artifacts/\\303\\251.sh"'
run_wrapper "$work"
assert_verdict 'case (cx6d)' PASS 0
assert_says 'case (cx6d) prompt-content recognises the C-quoted diff header' '^prompt-content: PASS \(2/2 code census paths present\)$'
assert_lacks 'case (cx6d) prompt-content does not false-FAIL on an octal-escaped path' '^prompt-content: FAIL'

printf '== case (cx6e): a NON-ASCII PROSE path is classified by its RAW bytes, not its quoted spelling ==\n'
reset_stub
# #3229 round 4, BLOCKER F1. The census read `git diff --numstat` WITHOUT `-z`, so this
# path arrived C-QUOTED and was classified by that spelling: extension `md"` (not `md`),
# prefix `"docs/…` (not `docs/`) ⇒ a MARKDOWN FILE counted as CODE. `*.md` then legitimately
# removes it from the reviewer's diff, so `prompt-content:` FAILed demanding a file the
# configuration had already excluded — on an ORDINARY docs+code branch. REPRODUCED against
# the repo's own tracked
# `docs/research/CQLite Writes (M5) — Analysis & Recommended Paths.md`.
# The pre-existing non-ASCII fixture is a `.sh` — CODE by accident — which is why no case
# covered this. This one is deliberately PROSE.
work=$(make_fixture case_cx6e docs-nonascii-prose)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_PROMPT='Review this diff:\ndiff --git a/main.rs b/main.rs'
run_wrapper "$work"
assert_verdict 'case (cx6e)' PASS 0
assert_says 'case (cx6e) prompt-content has exactly the one code path to look for' '^prompt-content: PASS \(1/1 code census paths present\)$'

printf '== case (cx6f): a NON-ASCII docs ARTIFACT is classified by its RAW bytes too ==\n'
reset_stub
# The same defect for the docs-scoped ARTIFACT half of the classification: quoted, the
# extension reads `json"`, so a report artifact counted as CODE while `docs/**/*.json`
# excludes it ⇒ the same false pre-enqueue FAIL on any report PR carrying a non-ASCII name.
work=$(make_fixture case_cx6f docs-nonascii-artifact)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_PROMPT='Review this diff:\ndiff --git a/main.rs b/main.rs'
run_wrapper "$work"
assert_verdict 'case (cx6f)' PASS 0

printf '== case (cx6g): a RENAME whose BOTH names carry a space is reachable ==\n'
reset_stub
# #3229 round 4, BLOCKER F2. The census runs `--no-renames` (two paths) while the
# reviewer's diff has rename detection ON (confirmed: `--no-renames` is absent from the
# roborev binary's strings), emitting ONE header `diff --git a/<old> b/<new>`. With a space
# in each name, step (a)'s `[^ ]+` regex cannot split it, step (b) requires BOTH sides
# quoted, and the literal fallback only probed the SAME-path header — so BOTH census sides
# were reported absent and `prompt-content:` FAILed a correct review.
work=$(make_fixture case_cx6g rename-space)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_PROMPT='Review this diff:\ndiff --git a/docs/storage engine/old probe.sh b/docs/storage engine/new probe.sh\nsimilarity index 100%'
run_wrapper "$work"
assert_verdict 'case (cx6g)' PASS 0
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
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_PROMPT='Review this diff:\ndiff --git a/docs/reports/x-artifacts/probe.sh "b/docs/reports/x-artifacts/\\303\\251 probe.sh"\nsimilarity index 100%'
run_wrapper "$work"
assert_verdict 'case (cx6h)' PASS 0
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
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_PROMPT='Review this diff:\ndiff --git a/a b/a\ndiff --git "a/a\\nb.rs" "b/a\\nb.rs"'
run_wrapper "$work"
assert_verdict 'case (cx6j)' PASS 0
assert_says 'case (cx6j) the quoted newline header counts as present' '^prompt-content: PASS \(2/2 code census paths present\)$'

printf '== case (cx6l): a space-bearing header does not prove an UNRELATED census path ==\n'
reset_stub
# #3229 round 5, BLOCKER 1 — a FALSE PASS in prompt-content:, the merge gate itself.
# REPRODUCED: `roborev_diff_header_has_path 'diff --git a/foo b/x b/foo b/x' foo` returned
# PRESENT, because the old membership test `case $rest in "a/$want b/"*)` is a PREFIX test
# and `a/foo b/` prefixes that header. So a repo tracking a file named `foo b/x` made the
# UNRELATED path `foo` read as delivered to the reviewer — a false PASS in the exact
# mechanism that certifies "the reviewer received the code". The prompt below carries ONLY
# the `foo b/x` header, so `foo` MUST be reported absent.
work=$(make_fixture case_cx6l ambiguous-space-pair)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_PROMPT='Review this diff:\ndiff --git a/foo b/x b/foo b/x'
run_wrapper "$work"
assert_verdict 'case (cx6l)' FAIL 1
assert_says 'case (cx6l) the unrelated path foo is reported ABSENT, not implied by a prefix' '^prompt-content: FAIL \(1/2 code census paths absent from the prompt\)$'
assert_lacks 'case (cx6l) never reports a full-coverage PASS on one header' '^prompt-content: PASS'

printf '== case (cx6m): an ambiguous RENAME header is resolved by its rename from/to lines ==\n'
reset_stub
# The mirror of (cx6l), and the reason the fix is not "prefer the equal split, full stop".
# The header `diff --git a/p b/x b/p b/x` admits an EQUAL split (`p b/x` == `p b/x`) that is
# NOT the true one, so equality alone reports BOTH real sides ABSENT (pinned as (cx6n)).
# git never leaves a rename ambiguous: it writes `rename from` / `rename to`, one path per
# line, and those lines are the authority the matcher resolves against.
work=$(make_fixture case_cx6m rename-ambiguous)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_PROMPT='Review this diff:\ndiff --git a/p b/x b/p b/x\nsimilarity index 100%\nrename from p\nrename to x b/p b/x\nindex e69de29..e69de29 100644'
run_wrapper "$work"
assert_verdict 'case (cx6m)' PASS 0
assert_says 'case (cx6m) the rename from/to lines cover both census sides' '^prompt-content: PASS \(2/2 code census paths present\)$'
assert_lacks 'case (cx6m) no false FAIL on an ambiguous rename header' '^prompt-content: FAIL'

printf '== case (cx6n): the SAME header WITHOUT rename lines cannot prove either side ==\n'
reset_stub
# So (cx6m) cannot be satisfied by anything other than the rename lines: strip them from the
# very same prompt and the header admits an equal split that matches NEITHER census path, so
# both must be reported absent. This is also the fail-closed direction of the (cx6l) fix —
# an ambiguous non-rename reading is never allowed to stand in for a real delivery.
work=$(make_fixture case_cx6n rename-ambiguous)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_PROMPT='Review this diff:\ndiff --git a/p b/x b/p b/x\nindex e69de29..e69de29 100644'
run_wrapper "$work"
assert_verdict 'case (cx6n)' FAIL 1
assert_says 'case (cx6n) without the rename lines neither side counts as present' '^prompt-content: FAIL \(2/2 code census paths absent from the prompt\)$'

printf '== case (cx6p): a filename cannot FORGE a summary key or the verdict ==\n'
reset_stub
# #3229 round 5, BLOCKER 2. Census paths are ATTACKER-CONTROLLED (whatever a PR branch
# tracks) and they are interpolated into LINE-ORIENTED summary values and DETAILS lines. A
# newline-bearing filename therefore let a value SPAN LINES and inject `key:` lines — up to a
# forged `RESULT: PASS` — into the very block flow-closer greps to decide whether to arm
# `--auto`. Neutralised centrally at the emit boundary: control characters become visible
# escapes, so no value and no DETAILS line can span a line.
#
# The surface it is exercised through is `prompt-content:` — the reviewer never receives the
# forged path, so the key FAILs and NAMES it. (Until #3283's subject was removed this case
# also went through a `census-exclusion:` FAIL value; the neutralisation being asserted is the
# SAME central boundary, exercised through the surface that remains.)
work=$(make_fixture case_cx6p newline-injection)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
run_wrapper "$work"
assert_verdict 'case (cx6p)' FAIL 1
assert_one_result_line 'case (cx6p)'
assert_one_block 'case (cx6p)'
assert_lacks 'case (cx6p) no forged RESULT: PASS anywhere in the output' '^RESULT: PASS'
assert_lacks 'case (cx6p) no forged prompt-content PASS' '^prompt-content: PASS'
assert_says 'case (cx6p) prompt-content reports the forged path as absent' '^prompt-content: FAIL \(1/2 code census paths absent from the prompt\)$'
# The path is still NAMED — neutralised, not dropped: the operator must be able to see WHICH
# file the reviewer did not get, on ONE line, with its newlines shown as visible escapes.
assert_says 'case (cx6p) the absent path is named with its newlines escaped, on one line' \
  '^  docs/reports/x-artifacts/harness/inj\\nRESULT: PASS\\nprompt-content: PASS\\nx\.sh$'

printf "== case (cx21): prompt-content: can NEVER emit a 0/0 PASS (direct unit probe) ==\n"
# A STRUCTURAL backstop, exercised DIRECTLY because the wrapper refuses the condition
# upstream: `code-free:` FAILs pre-enqueue on a census with no CODE path, so this branch is
# unreachable through the normal ordering — and that is exactly why it is probed directly.
# With no subject left, `PASS (0/0 code census paths present)` would be indistinguishable
# from a genuine pass, so the key must refuse to print one whatever happened upstream.
# Driven through the real function in the real files, so a future change to the upstream
# ordering cannot silently restore the vacuous PASS. (`CHECKS_SRC`/`ORACLES_SRC` are set at the
# top of this file — several direct probes need them.)
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
# NO code census path at all — the zero-subject condition, reached without any reference to
# any exclusion mechanism.
census_code_paths=()
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

printf '== case (cx28): the verdict grammar is CLOSED — an UNRECOGNISED value FAILs, it does not inherit PASS ==\n'
reset_stub
# THE GENERAL SHAPE, closed at the wrapper's single most consequential decision point
# (#3229 round-10 sweep). Three defects on this issue were ONE shape: a multi-state signal
# where only the BAD states are tested, so every unknown/unmeasured state inherits the
# PERMISSIVE branch (a three-state `built-in-set:` signal took the permissive excusal path —
# that subsystem is since deleted, #3278; `corroboration: UNAVAILABLE` reached a PASS —
# cx27a; a `${_census_end:-…}` fallback degraded a failed measurement to a 1-line scan). The
# verdict scan itself was the
# same shape: four failing prefixes tested, EVERYTHING else fell through to `finish PASS 0`.
#
# This case runs a PATCHED COPY of the three flow scripts in which ONE check reports a value
# outside the documented grammar — the observable signature of a check that aborted before
# assigning, or that invented a state the scan has never judged. It must FAIL and name itself.
#
# THE CONTROL RUNS FIRST, AND IT IS NOT OPTIONAL: an assert that a copy FAILs is satisfied by
# a copy that fails because it was copied wrong (a missing sibling file, a bad path), which is
# a probe failing in the direction that looks like success. So the UNPATCHED copy is shown to
# reach PASS on the same fixture before the patch is applied, and the patch is verified to have
# CHANGED the file before its run is believed.
_gm_dir="$tmp/grammar-mutant"
mkdir -p "$_gm_dir"
cp "$WRAPPER_REAL" "$SCRIPT_DIR/../flow/roborev-review-oracles.sh" \
  "$SCRIPT_DIR/../flow/roborev-review-checks.sh" "$_gm_dir/"
if [ -f "$SCRIPT_DIR/../flow/roborev-job-facts.py" ]; then
  cp "$SCRIPT_DIR/../flow/roborev-job-facts.py" "$_gm_dir/"
fi
work=$(make_fixture case_cx28 pushed)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
run_wrapper --wrapper "$_gm_dir/roborev-review.sh" "$work"
assert_verdict 'case (cx28 control) the UNPATCHED copy reaches PASS' PASS 0
assert_lacks 'case (cx28 control) and reports no grammar violation' 'verdict-grammar'
# ONE key, ONE value, outside the grammar. `MEASUREMENT-DID-NOT-HAPPEN` is deliberately not a
# near-miss of a recognised prefix, so the case pins the ALLOW-LIST rather than a spelling.
if sed_inplace_verified "$_gm_dir/roborev-review-checks.sh" \
  's/^    TIER1="PASS"$/    TIER1="MEASUREMENT-DID-NOT-HAPPEN"/' \
  '    TIER1="MEASUREMENT-DID-NOT-HAPPEN"' '    TIER1="PASS"'; then
  ok 'case (cx28): the unrecognised-verdict patch was really applied to the copy'
  run_wrapper --wrapper "$_gm_dir/roborev-review.sh" "$work"
  assert_verdict 'case (cx28)' FAIL 1
  assert_says 'case (cx28) the unrecognised value is named under its own diagnostic' "^ERROR: verdict-grammar: a per-check key holds a value outside the block's documented grammar: 'MEASUREMENT-DID-NOT-HAPPEN'\. "
  assert_says 'case (cx28) it explains that an unplanned value must not inherit the non-failing branch' 'rather than letting the unplanned value inherit the non-failing branch'
  assert_says 'case (cx28) an empty value is called out as the same defect' "An EMPTY value \(''\) is this same defect"
  assert_says 'case (cx28) the unrecognised value still reaches the block, never silently' '^vacuity-tier1: MEASUREMENT-DID-NOT-HAPPEN$'
else
  bad 'case (cx28): could not patch the copied checks file, so the unrecognised-verdict path was never exercised (a green run here would be a probe failing in the direction that looks like success)'
fi

printf '== case (cx29): a check that never ran cannot ride to PASS on its initial SKIP ==\n'
# The neighbouring hazard to cx28: a value that IS in the grammar and IS non-failing, but is
# not a MEASUREMENT. The wrapper validates that the checks file DEFINES its five functions —
# which proves they exist, not that each reached its assignment. A check that returns early
# leaves its key at the initial `SKIP`, and before the round-10 backstop the run PASSED with a
# verdict-carrying key that had measured nothing: the vacuous pass this wrapper exists to
# prevent, textually identical to a genuine one.
#
# The patch makes `roborev_check_prompt_content` — the STRONGEST anti-vacuity key — return
# before assigning anything, which is exactly what an aborted helper or a stray `return` in a
# new branch looks like. cx28's control (the unpatched copy PASSes on this fixture) is the
# both-directions control for this case too; the patch is verified applied before it is run.
# VERIFIED, not assumed, in BOTH halves (#3296): the edit must have CHANGED the file (the
# helper's status, read here rather than discarded) and the `return 0` must be the line
# IMMEDIATELY AFTER the function header. A sed that matched nothing leaves a copy identical to
# the control, and this case would then be asserting against an unpatched wrapper — a probe
# failing in the direction that looks like success.
_gm_patched=''
if sed_inplace "$_gm_dir/roborev-review-checks.sh" \
  's/^roborev_check_prompt_content() {$/roborev_check_prompt_content() {\
  return 0/'; then
  _gm_patched=$(grep -A1 '^roborev_check_prompt_content() {$' "$_gm_dir/roborev-review-checks.sh" \
    | sed -n '2p')
fi
if [ "$_gm_patched" != '  return 0' ]; then
  bad "case (cx29): could not patch the copied checks file for an early return (the line after the header reads '$_gm_patched'), so the never-ran-check path was never exercised"
# Undo cx28's patch so the ONLY grammar-relevant difference is the un-run check. This restore
# is REQUIRED to succeed and its post-state is MEASURED — `TIER1="PASS"` present AND cx28's
# invalid value gone. A restore that silently stopped matching leaves the copy carrying cx28's
# out-of-grammar value, and then cx29's own causal assert (`RESULT: FAIL`, exit 1) is satisfied
# by cx28's verdict-grammar violation instead of the un-run check this case is about: a
# stale-value pass, measured as such before this guard existed.
elif ! sed_inplace_verified "$_gm_dir/roborev-review-checks.sh" \
  's/^    TIER1="MEASUREMENT-DID-NOT-HAPPEN"$/    TIER1="PASS"/' \
  '    TIER1="PASS"' 'MEASUREMENT-DID-NOT-HAPPEN'; then
  bad 'case (cx29): the cx28 TIER1 mutation could not be verifiably restored to PASS on the copy, so a FAIL below would be cx28 grammar violation rather than cx29 never-ran check — the case is NOT MEASURED and must not report a pass'
else
  ok 'case (cx29): the early-return patch was really applied to the copy, and the cx28 TIER1 mutation was verified restored to PASS'
  run_wrapper --wrapper "$_gm_dir/roborev-review.sh" "$work"
  assert_verdict 'case (cx29)' FAIL 1
  assert_says 'case (cx29) the un-run key is named as never having affirmatively passed' "^ERROR: verdict-affirmation: this run reached the PASS branch with a VERDICT-CARRYING key that never affirmatively passed — prompt-content: 'SKIP'\. "
  assert_says 'case (cx29) it states that a non-measurement is the vacuous pass itself' 'a non-failing value that is not a measurement'
  assert_says 'case (cx29) it points the reader at the wrapper, not the branch under review' 'NOT something to fix in the branch under review'
  assert_says 'case (cx29) the un-run key is visible in the block' '^prompt-content: SKIP$'
  assert_lacks 'case (cx29) and the grammar check does not misreport it as unrecognised' 'verdict-grammar'
fi

# ===========================================================================
# (cx28b/cx28c): THE CLOSURE MUST NOT ITSELF BE A PREFIX TEST (#3229 round-11 M3).
#
# cx28 pins that a value OUTSIDE the allow-list FAILs. It cannot see the neighbouring
# defect, because its mutant (`MEASUREMENT-DID-NOT-HAPPEN`) is deliberately NOT a
# near-miss: the scan matched `PASS*` / `SKIP*` / … as PREFIX GLOBS, so any value merely
# BEGINNING with a recognised token was accepted AS that token and rode to `RESULT: PASS`.
# The closure was therefore checking a SPELLING, not a state — the same "absence of a bad
# word is not evidence of a good outcome" error it was written to remove, reintroduced
# inside itself one level down.
#
# TWO MUTANTS, both NEAR-PREFIXES of `PASS`, run through cx28's harness (whose UNPATCHED
# control already showed this fixture reaches PASS):
#   cx28b  PASSthisNeverRan             — a token glued to more characters, no separator
#   cx28c  PASS-MEASUREMENT-DID-NOT-HAPPEN — a token followed by a hyphenated state name
# Under the prefix form BOTH were accepted silently. Under exact token matching both are
# UNRECOGNISED and fail closed. `vacuity-tier1:` is the mutation site deliberately: it is in
# the grammar scan but NOT in the affirmation backstop, so a green here could ONLY come from
# the grammar arm — no other check can rescue the assert.
# ===========================================================================
for _np_case in cx28b:PASSthisNeverRan cx28c:PASS-MEASUREMENT-DID-NOT-HAPPEN; do
  _np_label="${_np_case%%:*}"
  _np_value="${_np_case#*:}"
  printf '== case (%s): the near-prefix value %s is UNRECOGNISED, not a PASS ==\n' "$_np_label" "$_np_value"
  reset_stub
  # Restore the copy to the control state, then apply ONLY this mutation.
  # The restore is the `cp` ALONE, and deliberately so (#3296): the line that used to sit
  # here tried to undo cx29's insertion with `sed -i 's/^header() {\n  return 0$/header() {/'`
  # — a TWO-LINE left-hand side, which no sed can match, since sed's pattern space holds one
  # line at a time. It was therefore a no-op on GNU too, and the wholesale `cp` from the real
  # checks file was already doing all the restoring. Removed rather than made portable: making
  # a no-op portable would have been inventing behaviour the suite never had.
  cp "$SCRIPT_DIR/../flow/roborev-review-checks.sh" "$_gm_dir/roborev-review-checks.sh"
  work=$(make_fixture "case_$_np_label" pushed)
  STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
  # THE CONTROL, per case: the restored copy must reach PASS on this fixture, or a FAIL below
  # would prove nothing about the mutation.
  run_wrapper --wrapper "$_gm_dir/roborev-review.sh" "$work"
  assert_verdict "case ($_np_label control) the restored copy reaches PASS" PASS 0
  if sed_inplace_verified "$_gm_dir/roborev-review-checks.sh" \
    "s/^    TIER1=\"PASS\"\$/    TIER1=\"$_np_value\"/" \
    "    TIER1=\"$_np_value\"" '    TIER1="PASS"'; then
    ok "case ($_np_label): the near-prefix patch was really applied to the copy"
    run_wrapper --wrapper "$_gm_dir/roborev-review.sh" "$work"
    assert_verdict "case ($_np_label)" FAIL 1
    assert_says "case ($_np_label) the near-prefix value is named as OUTSIDE the grammar" \
      "^ERROR: verdict-grammar: a per-check key holds a value outside the block's documented grammar: '$_np_value'\. "
    assert_says "case ($_np_label) the diagnostic states that the token is matched exactly" \
      'the token is matched EXACTLY, up to the value.s first space'
    assert_says "case ($_np_label) the value still reaches the block, never silently" \
      "^vacuity-tier1: $_np_value\$"
    assert_lacks "case ($_np_label) and it never reads as a PASS" '^RESULT: PASS'
  else
    bad "case ($_np_label): could not patch the copied checks file, so the near-prefix path was never exercised (a green run here would be a probe failing in the direction that looks like success)"
  fi
  done
cp "$SCRIPT_DIR/../flow/roborev-review-checks.sh" "$_gm_dir/roborev-review-checks.sh"

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
assert_says 'case (n) says the reviewer never received the diffs' 'nothing establishes that the reviewer received their diffs'
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
assert_says 'case (f) reviewed-sha reports the RANGE' "^reviewed-sha: $(git -C "$work" merge-base origin/main HEAD)\.\.$head_sha\$"
assert_says 'case (f) job printed' '^job: 4656$'
assert_says 'case (f) log path named' '^log: .+transcript-'
assert_one_block 'case (f)'
# PORTABILITY (#3296): compare CANONICAL to CANONICAL, computed by the SAME mechanism the
# wrapper uses. roborev-review.sh resolves --repo with
#   REPO=$(git -C "$REPO_ARG" rev-parse --show-toplevel); REPO=$(cd "$REPO" && pwd -P)
# and records THAT string. On macOS $TMPDIR lives under /var, which is a symlink to
# /private/var, so the fixture path `$work` and the recorded path differ by that prefix and
# the literal `--repo $work` match FAILed — with the wrapper behaving perfectly (this is what
# broke both case (f) asserts at pristine origin/main). The expectation is canonicalised the
# same way rather than loosened to a substring: the full flag string is still matched with
# `grep -F`, so a relative --repo, a missing --repo, or a root-checkout --repo still FAILs.
# >>> BEGIN case-f-invocation-asserts (#3296) — this block is EXTRACTED VERBATIM and
# mutation-tested by scripts/tests/test_roborev_guard_portability.sh, which feeds it a
# relative, a root-checkout and a missing `--repo` record and requires BOTH asserts to
# report `bad` for each. Keep the markers; the extractor FAILs closed if they go missing.
work_canon=$(cd "$(git -C "$work" rev-parse --show-toplevel)" && pwd -P)
# The sanctioned invocation form: explicit HEAD sha, explicit ABSOLUTE --repo,
# --wait, both --agent and --model — and never --branch, never two positionals.
# THE BASE IS MATCHED BY SHAPE HERE, AND BY VALUE JUST BELOW (#3392). The enqueue now pins the
# RESOLVED 40-hex merge-base rather than the symbolic ref, so the census, the enqueue, the assert and
# the waiver scope all name ONE immutable range. This block is EXTRACTED and mutation-tested by the
# portability suite against a fixture that has no `origin/main` at all, so it cannot compute the
# value — it asserts the SHAPE (`--base <40-hex>` immediately followed by `--repo`), which is what
# makes a symbolic ref, an empty base and a dropped `--repo` all fail here. The VALUE assert (that the
# sha is THIS fixture's merge-base) is the next assert down, outside the extracted region, where the
# fixture can compute it. Exactly TWO assert sites live in this block: the portability probes require
# dp==2 / df==2, so adding a third here would break their arithmetic.
if grep -qE -- '^review --branch --base [0-9a-f]{40} --repo ' "$INVOKED" \
  && grep -qF -- "--repo $work_canon --agent codex --model gpt-5.6-sol --wait" "$INVOKED"; then
  ok 'case (f): invoked over the census RANGE with a 40-hex pinned base and an explicit absolute --repo + --wait'
else
  bad "case (f): unexpected invocation form: $(cat "$INVOKED")"
fi
# `--branch` is correct ONLY with an explicit --repo (without it, it resolves against
# the ROOT checkout); the two-positional range form must never appear. `--repo` is matched
# with its ABSOLUTE canonical value, so a relative path would not satisfy it.
if grep -qF -- '--branch' "$INVOKED" && grep -qF -- "--repo $work_canon" "$INVOKED"; then
  ok 'case (f): --branch is paired with an explicit absolute --repo'
else
  bad "case (f): --branch/--repo pairing missing: $(cat "$INVOKED")"
fi
# <<< END case-f-invocation-asserts (#3296)
# THE VALUE PIN for the base the wrapper enqueued (#3392): it must be the MERGE-BASE of the base ref
# and HEAD — the base of the range the census measured — and NOT the symbolic ref, whose re-resolution
# by roborev is what let the enqueued range drift from the measured one between the two steps.
_cf_pinned_base=$(git -C "$work" merge-base origin/main HEAD)
if grep -qF -- "--base $_cf_pinned_base " "$INVOKED"; then
  ok 'case (f): the enqueued base is the RESOLVED merge-base sha, so census/enqueue/assert name one immutable range'
else
  bad "case (f): the enqueued base is not the resolved merge-base '$_cf_pinned_base': $(cat "$INVOKED")"
fi
if grep -qF -- '--base origin/main' "$INVOKED"; then
  bad 'case (f): the SYMBOLIC base ref reached the enqueue — roborev would re-resolve it, so the reviewed range could differ from the measured one'
else
  ok 'case (f): no symbolic base ref reaches the enqueue'
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
# Extracted with `summary_key_order` (one awk) — see its definition for WHY the previous
# `grep -n | cut | paste -sd,` form returned the EMPTY STRING on macOS (#3296): the
# operand-less `paste` usage()-errors on BSD instead of reading stdin. An empty extraction
# still FAILs this compare, so the fix changes a FALSE FAIL into a real measurement; it does
# not make the assert conditional.
_j2_key_order=$(summary_key_order "$OUT" 'vacuity-tier2|roborev-exit|log')
if [ "$_j2_key_order" = "vacuity-tier2,roborev-exit,log" ]; then
  ok 'case (j2): roborev-exit is positioned between vacuity-tier2 and log'
else
  bad "case (j2): unexpected key order: $_j2_key_order"
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

printf '== case (t6): a census whose RANGE cannot be measured is not "genuinely empty" ==\n'
reset_stub
# THE CLASS INVARIANT, which is what this case has always been for: "we could not measure the
# range" must never render as "there is nothing to review". Unrelated histories now abort one step
# EARLIER than they used to — at the merge-base resolution rather than at the `git diff` (#3392),
# because the range base is resolved before the diff that uses it — so this asserts the invariant
# and the pre-enqueue guarantee, while case (mb7) pins the merge-base diagnostic itself. The
# `git diff failed` arm is retained as a STRUCTURAL backstop for a diff that fails for some other
# reason (an unreadable object store), exactly like the other unreachable-by-ordering backstops in
# these files: the point of a backstop is not to depend on an upstream check still being there.
work=$(make_fixture case_t6 orphan-base)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
run_wrapper "$work" --base unrelated
assert_verdict 'case (t6)' FAIL 1
assert_says 'case (t6) the census check is what FAILs' '^census-check: FAIL \('
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
# HOME is redirected for the same reason `run_wrapper` does it: a hand-rolled invocation
# must be as hermetic as the helper, so a host `$HOME/.roborev/` can never influence it.
STUB_INVOKED="$INVOKED" PATH="$stubbin:$PATH" HOME="$FIXTURE_HOME" \
  bash "$WRAPPER_REAL" --repo "$tmp/case_t7/work" \
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
  STUB_INVOKED="$INVOKED" PATH="$nobin" "$nobin/bash" "$WRAPPER_REAL" --repo "$work" \
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
# HOME redirected (see case t7): hermeticity, so nothing on the host can fail this case on
# the wrong key and leave the EXIT-trap assertion below unreached.
STUB_INVOKED="$INVOKED" PATH="$stubbin:$PATH" HOME="$FIXTURE_HOME" \
  bash "$WRAPPER_REAL" --repo "$work" \
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
  STUB_INVOKED="$INVOKED" PATH="$stubbin:$PATH" bash "$WRAPPER_REAL" \
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
  STUB_INVOKED="$INVOKED" PATH="$stubbin:$PATH" bash "$WRAPPER_REAL" \
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
assert_says 'case (x1) says the reviewer never received the diffs' 'nothing establishes that the reviewer received their diffs'
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

# =============================================================================================
# (mb*) THE RANGE BASE IS THE MERGE-BASE, NEVER THE BASE REF'S TIP (issue #3392)
# =============================================================================================
# THE DEFECT THESE PIN. The census measures `<base>...HEAD`, which is
# `merge-base(<base>, HEAD)..HEAD`, and roborev reviews the same range, so its job record's
# `git_ref` carries the MERGE-BASE as the range base. `sha-assert` compared that against
# `rev-parse <base>` — the base ref's TIP — so it FAILED DETERMINISTICALLY for every branch
# whose base had advanced since the branch point, i.e. for almost every branch not just rebased.
# It was misdiagnosed as a race twice; the controlled measurement (the base ref recorded before
# AND after a failing review, unmoved) is what killed that hypothesis.
#
# WHY EVERY CASE HERE CALLS `assert_base_advanced` FIRST. Under the OLD code these cases would
# pass just as well in a fixture where the tip and the merge-base coincide — the distinction
# would be untested and the family would be decoration. The inequality is therefore MEASURED
# per case, not assumed from the fixture's name.
#
# AND WHY THE FAMILY IS SIX CASES, NOT ONE. A guard tested only in its passing direction is how
# a vacuous green ships: (mb1) is the headline PASS, and (mb2)-(mb6) are the discriminations —
# a stale head, the T2 empty-tree base, the T1 tip equality, a branch-point equality, and the
# tip-anchored RANGE the old assert used to demand — every one of which must still FAIL.
printf '== case (mb1): a CORRECT review of a branch whose base has advanced PASSes ==\n'
reset_stub
work=$(make_fixture case_mb1 advanced-base)
assert_base_advanced 'case (mb1)' "$work"
mb_tip=$(git -C "$work" rev-parse origin/main)
mb_base=$(git -C "$work" merge-base origin/main HEAD)
mb_head=$(git -C "$work" rev-parse HEAD)
# The record roborev really writes for this branch: the MERGE-BASE, then branch HEAD. The
# announcement names only the range base, which for a range review is that same merge-base.
STUB_ANNOUNCE_SHA="$mb_base"
STUB_GIT_REF="$mb_base..$mb_head"
run_wrapper "$work"
assert_verdict 'case (mb1)' PASS 0
assert_says 'case (mb1) sha-assert PASSes on a correct review of a branch that is behind' '^sha-assert: PASS$'
assert_says 'case (mb1) reviewed-sha is the merge-base-anchored range' "^reviewed-sha: $mb_base\.\.$mb_head\$"
# AC3: the block STATES which base the assert compared against, and prints the tip beside it, so a
# reader of a pasted block can tell the two apart instead of inferring which one `base:` meant.
assert_says 'case (mb1) assert-base names the MERGE-BASE, with the tip beside it' \
  "^assert-base: $mb_base \(merge-base of origin/main and HEAD; origin/main tip $mb_tip\)\$"
assert_lacks 'case (mb1) the TIP is not what the assert compared against' "^assert-base: $mb_tip "
# Key POSITION is part of the block contract (see case (j2)): `assert-base:` sits with the other
# endpoints, between `reviewed-sha:` and `job:`, where a reader compares them.
_mb1_key_order=$(summary_key_order "$OUT" 'head-sha|reviewed-sha|assert-base|job')
if [ "$_mb1_key_order" = "head-sha,reviewed-sha,assert-base,job" ]; then
  ok 'case (mb1): assert-base is positioned between reviewed-sha and job'
else
  bad "case (mb1): unexpected key order: $_mb1_key_order"
fi

printf '== case (mb2): with the base advanced, a STALE HEAD still FAILs (negative control) ==\n'
reset_stub
# THE CONTROL FOR (mb1): same fixture, same advanced base, the ONLY difference being a reviewed
# range that stops one commit short of the branch tip. If (mb1) passed because the assert stopped
# checking, this would pass too. It must not.
work=$(make_fixture case_mb2 advanced-base)
assert_base_advanced 'case (mb2)' "$work"
mb_base=$(git -C "$work" merge-base origin/main HEAD)
mb_head=$(git -C "$work" rev-parse HEAD)
mb_stale=$(git -C "$work" rev-parse 'HEAD^')
STUB_ANNOUNCE_SHA="$mb_base"
STUB_GIT_REF="$mb_base..$mb_stale"
run_wrapper "$work"
assert_verdict 'case (mb2)' FAIL 1
assert_says 'case (mb2) sha-assert still FAILs on a short range' '^sha-assert: FAIL \(reviewed range does not match origin/main\.\.\.HEAD\)$'
assert_says 'case (mb2) the short HEAD endpoint is named' "the range HEAD is '$mb_stale', not branch HEAD '$mb_head'"
assert_says 'case (mb2) it says the scope stopped short' 'the reviewed scope stops short of the branch tip'

printf '== case (mb3): with the base advanced, an EMPTY-TREE base still FAILs (T2 pin) ==\n'
reset_stub
# T2 is a property of the range base VALUE, so advancing the base ref must not disturb it.
work=$(make_fixture case_mb3 advanced-base)
assert_base_advanced 'case (mb3)' "$work"
mb_base=$(git -C "$work" merge-base origin/main HEAD)
mb_head=$(git -C "$work" rev-parse HEAD)
STUB_ANNOUNCE_SHA="$mb_base"
STUB_GIT_REF="4b825dc642cb6eb9a060e54bf8d69288fbee4904..$mb_head"
run_wrapper "$work"
assert_verdict 'case (mb3)' FAIL 1
assert_says 'case (mb3) the wrong range BASE is named' 'the range BASE is .4b825dc642cb6eb9a060e54bf8d69288fbee4904.'
assert_says 'case (mb3) the empty-tree signature survives the change' 'empty-tree base \(4b825dc6\.\.\.\) is the signature of the non-sanctioned two-positional'
assert_says 'case (mb3) the expected base is the merge-base' "not '$mb_base'"

printf '== case (mb4): a single-sha record equal to the TIP still FAILs (T1 pin) ==\n'
reset_stub
# AC2, AND THE REASON THE TIP IS STILL READ AT ALL. A `--branch` review resolved against the ROOT
# checkout enqueues the base ref's TIP — so the tip, not the merge-base, is the sha this signature
# is about. With the base advanced the two are DIFFERENT commits, which is precisely the fixture in
# which a merge-base-only implementation would lose the T1 diagnostic.
work=$(make_fixture case_mb4 advanced-base)
assert_base_advanced 'case (mb4)' "$work"
mb_tip=$(git -C "$work" rev-parse origin/main)
mb_base=$(git -C "$work" merge-base origin/main HEAD)
STUB_ANNOUNCE_SHA="$mb_tip"
STUB_GIT_REF="$mb_tip"
run_wrapper "$work"
assert_verdict 'case (mb4)' FAIL 1
assert_says 'case (mb4) sha-assert FAILs' '^sha-assert: FAIL \(reviewed-sha does not match head-sha\)$'
assert_says 'case (mb4) the TIP equality is named as such' "the reviewed sha EQUALS the TIP of the base ref 'origin/main' \($mb_tip\)"
assert_says 'case (mb4) the root-checkout resolution is still called out' "signature of a '--branch' review resolved against the ROOT checkout"
assert_says 'case (mb4) it also names the base the range SHOULD have had' "the merge-base, $mb_base"

printf '== case (mb5): a single-sha record equal to the MERGE-BASE also FAILs ==\n'
reset_stub
# The other base-equality: a review anchored at the branch point itself reviewed nothing either,
# because every commit under review is a DESCENDANT of it. Both equalities FAIL; the diagnostic
# says WHICH one matched, so an operator can tell a root-checkout resolution from a branch-point one.
work=$(make_fixture case_mb5 advanced-base)
assert_base_advanced 'case (mb5)' "$work"
mb_tip=$(git -C "$work" rev-parse origin/main)
mb_base=$(git -C "$work" merge-base origin/main HEAD)
STUB_ANNOUNCE_SHA="$mb_base"
STUB_GIT_REF="$mb_base"
run_wrapper "$work"
assert_verdict 'case (mb5)' FAIL 1
assert_says 'case (mb5) sha-assert FAILs' '^sha-assert: FAIL \(reviewed-sha does not match head-sha\)$'
assert_says 'case (mb5) the merge-base equality is named' "the reviewed sha EQUALS the MERGE-BASE of 'origin/main' and HEAD \($mb_base\)"
assert_says 'case (mb5) it explains why that reviewed nothing' 'the branch point itself, so NO branch change was reviewed'
assert_says 'case (mb5) the tip is reported beside it' "The tip of 'origin/main' is $mb_tip"

printf '== case (mb6): a RANGE anchored at the base ref TIP FAILs (the old expectation) ==\n'
reset_stub
# THE DIRECTION THAT PROVES THE FIX IS NOT A LOOSENING. `<tip>..HEAD` is exactly the range the
# OLD assert demanded, and it is NOT the branch diff — it subtracts commits that only main has.
# So the shape the old code required must now FAIL, or "compare against the merge-base" would
# just mean "accept either".
work=$(make_fixture case_mb6 advanced-base)
assert_base_advanced 'case (mb6)' "$work"
mb_tip=$(git -C "$work" rev-parse origin/main)
mb_base=$(git -C "$work" merge-base origin/main HEAD)
mb_head=$(git -C "$work" rev-parse HEAD)
STUB_ANNOUNCE_SHA="$mb_tip"
STUB_GIT_REF="$mb_tip..$mb_head"
run_wrapper "$work"
assert_verdict 'case (mb6)' FAIL 1
assert_says 'case (mb6) a tip-anchored range is refused' '^sha-assert: FAIL \(reviewed range does not match origin/main\.\.\.HEAD\)$'
assert_says 'case (mb6) the expected base is the merge-base' "the range BASE is '$mb_tip', not '$mb_base'"
assert_says 'case (mb6) the tip-anchor is diagnosed by name' "A base equal to the TIP of 'origin/main' \($mb_tip\)"

printf '== case (mb7): an UNRESOLVABLE merge-base FAILs CLOSED, pre-enqueue ==\n'
reset_stub
# AFFIRMATIVE MEASUREMENT. With no common ancestor the base OF THE RANGE is unknown, and an
# unknown base must never degrade to the tip (the tip is the defect) or to an empty string (which
# would make the assert compare against nothing). It is a census FAIL, before any review is
# enqueued, so it costs no review tokens.
work=$(make_fixture case_mb7 orphan-base)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
run_wrapper "$work" --base unrelated
assert_verdict 'case (mb7)' FAIL 1
assert_says 'case (mb7) the unresolvable merge-base is named' "^census-check: FAIL \(no merge-base between 'unrelated' and HEAD\)\$"
assert_says 'case (mb7) it is explicitly not NOTHING-TO-REVIEW' 'explicitly NOT a NOTHING-TO-REVIEW'
assert_says 'case (mb7) it refuses to degrade to the tip' 'deliberately NOT degraded to the tip'
assert_lacks 'case (mb7) never reports NOTHING-TO-REVIEW' '^RESULT: NOTHING-TO-REVIEW$'
assert_never_enqueued 'case (mb7)'


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
cp "$WRAPPER_REAL" "$lonely_checks/roborev-review.sh"
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
cp "$WRAPPER_REAL" "$trunc_checks/roborev-review.sh"
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
cp "$WRAPPER_REAL" "$lonely/roborev-review.sh"
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
cp "$WRAPPER_REAL" "$truncated/roborev-review.sh"
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
  STUB_INVOKED="$INVOKED" PATH="$stubbin:$PATH" bash "$WRAPPER_REAL" --repo "$work" $pair >"$OUT" 2>&1
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

# =============================================================================================
# (wv*) ONE QUESTION, NO CLASSIFIER — AND A HUMAN-AUTHORIZED WAIVER (issue #3312, ruling (4))
# =============================================================================================
# WHAT THIS FAMILY REPLACED, so nobody rebuilds it. The wrapper used to infer HOW roborev delivered
# the diff — inline, or by a path to a transient snapshot file, or the delegated tier that ships
# neither — and this file used to carry ~20 cases pinning that inference. FOUR consecutive review
# rounds each found a High-severity false verdict in it, in both directions, and every one had the
# same cause: structure inferred from prompt text that embeds repository-controlled content. The
# owner deleted the inference. So there is nothing here about blocks, headings, fences,
# mixed-delivery, candidate lifetime or snapshot paths: those states no longer exist to be tested.
#
# WHAT IS PINNED NOW: present ⇒ PASS, absent ⇒ FAIL whatever the prompt looks like, and the ONLY way
# past an absence is a waiver comment that names THIS head sha and a reason — with every non-granting
# state (none / stale / malformed / unavailable) leaving the FAIL in place under its own name, and the
# waiver unable to touch any other verdict.
w_work=$(make_fixture case_w two-code-commits)
w_head=$(git -C "$w_work" rev-parse HEAD)
# THE MERGE-BASE, because that is the base of the reviewed range and therefore the base the waiver
# scope is bound to (#3392). This fixture's base ref has not advanced, so it is the same commit as
# the tip here — case (mb8) is the one that separates them.
w_base=$(git -C "$w_work" merge-base origin/main HEAD)

printf '== (wv1) a prompt naming a snapshot path is NOT special: an absent census path FAILs ==\n'
# The shape that used to produce an exempted NOTICE. There is no snapshot mode any more, so this is
# just a prompt whose census paths are absent — a FAIL, and no `waiver:` grant.
reset_stub
STUB_ANNOUNCE_SHA="$w_head"
STUB_PROMPT='Review the change.\n\n### Combined Diff\n\n(Diff too large to include inline)\nRead the diff from: `'"$w_work"'/.roborev/roborev-snapshot-157393586/roborev-snapshot-content.diff`'
run_wrapper "$w_work"
assert_verdict 'case (wv1)' FAIL 1
assert_says 'case (wv1) absence is a FAIL, with no delivery-mode excuse' \
  '^prompt-content: FAIL \(2/2 code census paths absent from the prompt\)$'
assert_says 'case (wv1) the machine says it cannot tell WHY they are absent' \
  'THE MACHINE CANNOT TELL WHY THEY ARE ABSENT'
# LAYER 3 (job 23): the diagnostic points at --help and carries NO part of the marker, because a summary
# block pasted into a PR comment would otherwise authorize the next run — which is how it self-granted.
assert_says 'case (wv1) the diagnostic points at --help instead of printing a marker' \
  'THE EXACT MARKER FORM IS DELIBERATELY NOT PRINTED HERE'
assert_lacks 'case (wv1) and no part of the marker appears anywhere in the output' 'roborev-waive'
assert_says 'case (wv1) it names the review scope a waiver would have to bind' \
  "base $w_base — the merge-base of origin/main and HEAD, which is the base of the reviewed range and NOT the tip of origin/main, head $w_head, job 4656"
assert_says 'case (wv1) the NONE cause names the SHAPE requirement (job 29)' \
  'the marker must be the SOLE NONBLANK CONTENT of a TOP-LEVEL PR comment'
assert_says 'case (wv1) and it names the contexts that do not count' \
  'a marker inside prose, a code fence, a quote or a review body is not read'
assert_says 'case (wv1) the waiver state is reported as NONE' \
  '^waiver: NONE \(no waiver comment for this review: the marker must be the SOLE NONBLANK CONTENT of a TOP-LEVEL PR comment — a marker inside prose, a code fence, a quote or a review body is not read\)$'
assert_lacks 'case (wv1) no NOTICE verdict exists for this key any more' '^prompt-content: NOTICE'
assert_lacks 'case (wv1) and no snapshot keys are emitted' '^snapshot-'
reset_stub

printf '== (wv2) the delegated oversize tier FAILs too, with quoted headers present ==\n'
# The job-20 shape: roborev ships neither a diff nor a path, and repository content quotes headers
# that cover the census. Under the classifier this needed an ordering fix to avoid a PASS. Now the
# quoted headers ARE the prompt content, so they satisfy the one question that is asked — which is
# why the honest answer is that this case is no longer distinguishable, and the block says so.
reset_stub
STUB_ANNOUNCE_SHA="$w_head"
STUB_PROMPT='## Project Guidelines\nExample header:\ndiff --git a/alpha.rs b/alpha.rs\n\n### Combined Diff\n\n(Diff too large to include inline)\nFor Codex, inspect locally with `git diff HEAD~1`.'
run_wrapper "$w_work"
assert_verdict 'case (wv2)' FAIL 1
assert_says 'case (wv2) the path the prompt never carried is still absent' \
  '^prompt-content: FAIL \(1/2 code census paths absent from the prompt\)$'
assert_says 'case (wv2) and names it' '^  beta\.rs$'
reset_stub

printf '== (wv3) present ⇒ PASS, and the waiver key has no subject so it is ABSENT ==\n'
reset_stub
STUB_ANNOUNCE_SHA="$w_head"
STUB_PROMPT='### Combined Diff\n\ndiff --git a/alpha.rs b/alpha.rs\n@@ x @@\ndiff --git a/beta.rs b/beta.rs\n@@ y @@'
run_wrapper "$w_work"
assert_verdict 'case (wv3)' PASS 0
assert_says 'case (wv3) the certification spelling is unchanged' \
  '^prompt-content: PASS \(2/2 code census paths present\)$'
assert_lacks 'case (wv3) no waiver was looked for, so no waiver key is printed' '^waiver:'
assert_lacks 'case (wv3) and gh was never called' '^gh '
reset_stub

printf '== (wv4) a GRANTED waiver: WAIVED, RESULT PASS, and the provenance is recorded ==\n'
# (a)+(c): a human-authored PR comment naming THIS head excuses the absence — and the block records
# what was absent, who authorized it, for which sha and why. Never silence.
reset_stub
STUB_ANNOUNCE_SHA="$w_head"
STUB_PROMPT="$PROMPT_WITHOUT_PATHS"
STUB_GH_COMMENTS="\001pmcfadin\nroborev-waive: prompt-content-absent base=$w_base head=$w_head job=4656 reason=snapshot-delivered, 541k input / 472k cached\n"
run_wrapper "$w_work"
assert_verdict 'case (wv4)' PASS 0
assert_says 'case (wv4) the verdict token is WAIVED, not PASS' \
  "^prompt-content: WAIVED \(2/2 code census paths absent — authorized by @pmcfadin for base=$w_base head=$w_head job=4656\)\$"
assert_lacks 'case (wv4) a waived run must NOT read as a certification' '^prompt-content: PASS'
assert_says 'case (wv4) the waiver key records author, the whole scope and the reason' \
  "^waiver: GRANTED \(author=@pmcfadin base=$w_base head=$w_head job=4656 reason=snapshot-delivered, 541k input / 472k cached\)\$"
assert_says 'case (wv4) the absent paths are still listed' '^  alpha\.rs$'
assert_says 'case (wv4) and the authorship limitation is stated, not implied' \
  'PROCESS-ENFORCED WITH AN AUDIT TRAIL, NOT MECHANICALLY VERIFIED'
assert_says 'case (wv4) the limitation is SCOPED to which allowlisted human, not authorship in general' \
  'WHICH ALLOWLISTED HUMAN'
assert_says 'case (wv4) and the authorization that IS enforced is named' \
  'AUTHORIZED AGAINST AN EXPLICIT ALLOWLIST'
reset_stub

printf '== (wv5) (b) SHA-BOUND: a waiver for another head is STALE and does not excuse ==\n'
reset_stub
STUB_ANNOUNCE_SHA="$w_head"
STUB_PROMPT="$PROMPT_WITHOUT_PATHS"
STUB_GH_COMMENTS="\001pmcfadin\nroborev-waive: prompt-content-absent base=$w_base head=0000000000000000000000000000000000000000 job=4656 reason=stale one\n"
run_wrapper "$w_work"
assert_verdict 'case (wv5)' FAIL 1
assert_says 'case (wv5) the absence still FAILs' \
  '^prompt-content: FAIL \(2/2 code census paths absent from the prompt\)$'
assert_says 'case (wv5) and the divergent field is named' \
  "^waiver: STALE \(the marker names a different review — head \(0000000000000000000000000000000000000000 != $w_head\)"
assert_says 'case (wv5) the diagnostic names how to re-decide that job without re-reviewing' \
  'can be re-decided with --recheck-job'
assert_lacks 'case (wv5) a stale marker never yields WAIVED' '^prompt-content: WAIVED'
reset_stub

printf '== (wv6) a marker with no reason= is MALFORMED and does not excuse ==\n'
reset_stub
STUB_ANNOUNCE_SHA="$w_head"
STUB_PROMPT="$PROMPT_WITHOUT_PATHS"
STUB_GH_COMMENTS="\001pmcfadin\nroborev-waive: prompt-content-absent base=$w_base head=$w_head job=4656\n"
run_wrapper "$w_work"
assert_verdict 'case (wv6)' FAIL 1
assert_no_marker_form 'case (wv6)'
# A MISSING FIELD IS NOW REFUSED BY THE SINGLE ANCHORED PATTERN rather than by a per-field check, so
# every shape violation reports the same cause: the required form.
assert_says 'case (wv6) a reasonless marker does not match the required form' \
  '^waiver: MALFORMED \(the line begins an authorization of this kind but does not match its required form'
assert_lacks 'case (wv6) and never yields WAIVED' '^prompt-content: WAIVED'
reset_stub

printf '== (wv7) a gh failure (no PR / no auth / API error) is UNAVAILABLE, and FAILs closed ==\n'
reset_stub
STUB_ANNOUNCE_SHA="$w_head"
STUB_PROMPT="$PROMPT_WITHOUT_PATHS"
STUB_GH_RC=1
STUB_GH_COMMENTS="\001pmcfadin\nroborev-waive: prompt-content-absent base=$w_base head=$w_head job=4656 reason=would have been granted\n"
run_wrapper "$w_work"
assert_verdict 'case (wv7)' FAIL 1
assert_says 'case (wv7) an unreadable PR cannot grant a waiver' \
  '^waiver: UNAVAILABLE .*gh pr view --json comments'
assert_says 'case (wv7) the absence FAIL stands' \
  '^prompt-content: FAIL \(2/2 code census paths absent from the prompt\)$'
reset_stub

printf '== (wv8) (c) the waiver excuses the ABSENCE ONLY — another cause still FAILs ==\n'
# The constraint that keeps the waiver from becoming a general override: here the transcript carries no
# terminal verdict marker, so `review-completed` FAILs. A granted absence waiver must not rescue it.
reset_stub
STUB_ANNOUNCE_SHA="$w_head"
STUB_PROMPT="$PROMPT_WITHOUT_PATHS"
STUB_VERDICT='Waiting for job 4656 to complete...'
STUB_GH_COMMENTS="\001pmcfadin\nroborev-waive: prompt-content-absent base=$w_base head=$w_head job=4656 reason=absence is legitimate\n"
run_wrapper "$w_work"
assert_verdict 'case (wv8)' FAIL 1
assert_says 'case (wv8) the absence itself is waived' '^prompt-content: WAIVED'
assert_says 'case (wv8) but the other cause still FAILs the run' '^review-completed: FAIL'
reset_stub

printf '== (wv9) the LAST granted marker wins, so a re-request supersedes a stale one ==\n'
reset_stub
STUB_ANNOUNCE_SHA="$w_head"
STUB_PROMPT="$PROMPT_WITHOUT_PATHS"
STUB_GH_COMMENTS="\001pmcfadin\nroborev-waive: prompt-content-absent base=$w_base head=1111111111111111111111111111111111111111 job=4656 reason=before the push\n\001pmcfadin\nroborev-waive: prompt-content-absent base=$w_base head=$w_head job=4656 reason=re-requested after the push\n"
run_wrapper "$w_work"
assert_verdict 'case (wv9)' PASS 0
assert_says 'case (wv9) the current marker grants' "^waiver: GRANTED \(author=@pmcfadin base=$w_base head=$w_head job=4656 reason=re-requested after the push\)\$"
reset_stub

printf '== (wv10) JOB 29: a marker PLUS PROSE in one comment does NOT grant ==\n'
# THE NEW NEGATIVE THAT PAIRS WITH THE POSITIVE CONTROL (wv4). Under the sole-content rule the marker must be
# the ONLY nonblank line of its comment, so the commonest well-meant shape — a sentence of explanation above
# the marker — is not an authorization. That pair is what proves the sole-content rule is doing the deciding:
# same author, same scope, same reason, and the ONLY difference is the extra line.
reset_stub
STUB_ANNOUNCE_SHA="$w_head"
STUB_PROMPT="$PROMPT_WITHOUT_PATHS"
STUB_GH_COMMENTS="\001pmcfadin\nToken accounting checked: 541812 in / 472576 cached, genuine.\nroborev-waive: prompt-content-absent base=$w_base head=$w_head job=4656 reason=token accounting checked\n"
run_wrapper "$w_work"
assert_verdict 'case (wv10)' FAIL 1
assert_says 'case (wv10) a marker buried in prose is not an authorization' \
  '^waiver: NONE \(no waiver comment for this review: the marker must be the SOLE NONBLANK CONTENT'
assert_lacks 'case (wv10) and it does not grant' '^prompt-content: WAIVED'
reset_stub

printf '== (wv10b) JOB 29: the SANCTIONED workflow — commentary and authorization as SEPARATE comments ==\n'
# The cost of the rule, and the proof it is trivial: the authorizer explains in one comment (fenced example
# included) and authorizes in another. The second comment is marker-only, so it grants.
reset_stub
STUB_ANNOUNCE_SHA="$w_head"
STUB_PROMPT="$PROMPT_WITHOUT_PATHS"
STUB_GH_COMMENTS_JSON=$(SOLE_BASE="$w_base" SOLE_HEAD="$w_head" python3 -c '
import json, os
marker = ("roborev-waive: prompt-content-absent base=%s head=%s job=4656 reason=token accounting checked"
          % (os.environ["SOLE_BASE"], os.environ["SOLE_HEAD"]))
doc = "Token accounting checked: 541812 in / 472576 cached. The form is:\n\n```\n" + marker + "\n```"
print(json.dumps({"comments": [
    {"author": {"login": "pmcfadin"}, "body": doc},
    {"author": {"login": "pmcfadin"}, "body": marker}]}))
')
run_wrapper "$w_work"
assert_verdict 'case (wv10b)' PASS 0
assert_says 'case (wv10b) the marker-only comment grants; the documentation comment is inert' \
  "^waiver: GRANTED \(author=@pmcfadin base=$w_base head=$w_head job=4656 reason=token accounting checked\)\$"
reset_stub

printf '== (wv11) BLOCKER 1: the failure diagnostic REPOSTED as a PR comment must not waive ==\n'
# THE SHARPEST INSTANCE OF THE RECURRING SHAPE ON THIS ISSUE (roborev job 23): AN ARTIFACT THAT DESCRIBES THE
# ESCAPE HATCH BECAME THE ESCAPE HATCH. The diagnostic used to print a complete marker carrying the live
# sha, and detection accepted it anywhere inside a flattened comment — so pasting the summary block into a
# PR comment, which is the documented practice throughout this repo, authorized the next run.
#
# THE FIXTURE IS THE EXPLOIT, VERBATIM: run the wrapper on an absent-census prompt, take its ENTIRE output
# (block + diagnostics), post it as a PR comment, and run again. It must still be unwaived. The comment is
# fed through a FILE so nothing is re-escaped on the way in.
reset_stub
STUB_ANNOUNCE_SHA="$w_head"
STUB_PROMPT="$PROMPT_WITHOUT_PATHS"
run_wrapper "$w_work"
cp "$OUT" "$tmp/reposted-diagnostic.txt"
reset_stub
STUB_ANNOUNCE_SHA="$w_head"
STUB_PROMPT="$PROMPT_WITHOUT_PATHS"
{ printf '\001pmcfadin\n'; cat "$tmp/reposted-diagnostic.txt"; } >"$tmp/reposted-comment.txt"
STUB_GH_COMMENTS_FILE="$tmp/reposted-comment.txt"
run_wrapper "$w_work"
assert_verdict 'case (wv11)' FAIL 1
assert_says 'case (wv11) reposting the diagnostic does not waive anything' \
  '^prompt-content: FAIL \(2/2 code census paths absent from the prompt\)$'
assert_says 'case (wv11) and no waiver is found in it' \
  '^waiver: NONE \(no waiver comment for this review: the marker must be the SOLE NONBLANK CONTENT of a TOP-LEVEL PR comment — a marker inside prose, a code fence, a quote or a review body is not read\)$'
assert_lacks 'case (wv11) the reposted block never grants' '^prompt-content: WAIVED'
reset_stub

printf '== (wv12) LAYER 1: an indented, quoted or embedded marker copy does not match ==\n'
# Anchoring, case by case: a `>`-quoted line (a GitHub reply), an indented code-block copy, a bulleted
# copy, and a marker embedded mid-sentence. Every one of them is a way a HUMAN legitimately quotes the
# form while discussing it, so every one of them must be inert.
reset_stub
STUB_ANNOUNCE_SHA="$w_head"
STUB_PROMPT="$PROMPT_WITHOUT_PATHS"
STUB_GH_COMMENTS="\001pmcfadin\n> roborev-waive: prompt-content-absent base=$w_base head=$w_head job=4656 reason=quoted reply\n    roborev-waive: prompt-content-absent base=$w_base head=$w_head job=4656 reason=indented copy\n- roborev-waive: prompt-content-absent base=$w_base head=$w_head job=4656 reason=bulleted copy\nthe form is roborev-waive: prompt-content-absent base=$w_base head=$w_head job=4656 reason=embedded mid-sentence\n"
run_wrapper "$w_work"
assert_verdict 'case (wv12)' FAIL 1
assert_says 'case (wv12) no quoted or indented copy is honoured' \
  '^waiver: NONE \(no waiver comment for this review: the marker must be the SOLE NONBLANK CONTENT of a TOP-LEVEL PR comment — a marker inside prose, a code fence, a quote or a review body is not read\)$'
assert_lacks 'case (wv12) and none of them grants' '^prompt-content: WAIVED'
reset_stub

printf '== (wv13) LAYER 2: a pasted TEMPLATE with placeholder fields is MALFORMED ==\n'
# The documentation own line, anchored and complete in shape, must still refuse: an unsubstituted
# `<why>` is the signature of a paste rather than a judgment. Same rule `claim.sh --reason` applies.
reset_stub
STUB_ANNOUNCE_SHA="$w_head"
STUB_PROMPT="$PROMPT_WITHOUT_PATHS"
STUB_GH_COMMENTS="\001pmcfadin\nroborev-waive: prompt-content-absent base=$w_base head=$w_head job=4656 reason=<why>\n"
run_wrapper "$w_work"
assert_verdict 'case (wv13)' FAIL 1
assert_says 'case (wv13) an unsubstituted placeholder is refused by name' \
  '^waiver: MALFORMED \(the marker is missing a-substituted-reason'
assert_lacks 'case (wv13) and never grants' '^prompt-content: WAIVED'
reset_stub

printf '== (wv14) LAYER 2: a bare placeholder reason (why/todo/tbd) is MALFORMED ==\n'
reset_stub
STUB_ANNOUNCE_SHA="$w_head"
STUB_PROMPT="$PROMPT_WITHOUT_PATHS"
STUB_GH_COMMENTS="\001pmcfadin\nroborev-waive: prompt-content-absent base=$w_base head=$w_head job=4656 reason=TODO\n"
run_wrapper "$w_work"
assert_verdict 'case (wv14)' FAIL 1
assert_says 'case (wv14) a bare placeholder is refused by name' \
  "^waiver: MALFORMED \(the marker is missing a-substantive-reason \(the reason 'TODO' is a bare placeholder\)"
reset_stub

printf '== (wv15) BLOCKER 2: a waiver for the same head but a DIFFERENT base does not carry over ==\n'
# The scope binding. The authorizer judged ONE review; a different base is a different census, so the
# marker must not survive it even though the head is identical.
reset_stub
STUB_ANNOUNCE_SHA="$w_head"
STUB_PROMPT="$PROMPT_WITHOUT_PATHS"
STUB_GH_COMMENTS="\001pmcfadin\nroborev-waive: prompt-content-absent base=2222222222222222222222222222222222222222 head=$w_head job=4656 reason=judged against another base\n"
run_wrapper "$w_work"
assert_verdict 'case (wv15)' FAIL 1
assert_says 'case (wv15) the base divergence is named' \
  "^waiver: STALE \(the marker names a different review — base \(2222222222222222222222222222222222222222 != $w_base\)"
assert_lacks 'case (wv15) and it does not grant' '^prompt-content: WAIVED'
reset_stub

printf '== (wv16) BLOCKER 2: a waiver for a DIFFERENT job (a re-run) does not carry over ==\n'
# One persistent comment must not waive a later, possibly VACUOUS review at the same base and head.
reset_stub
STUB_JOB=9999
STUB_ANNOUNCE_SHA="$w_head"
STUB_PROMPT="$PROMPT_WITHOUT_PATHS"
STUB_GH_COMMENTS="\001pmcfadin\nroborev-waive: prompt-content-absent base=$w_base head=$w_head job=4656 reason=judged the earlier run\n"
run_wrapper "$w_work"
assert_verdict 'case (wv16)' FAIL 1
assert_says 'case (wv16) the job divergence is named' \
  "^waiver: STALE \(the marker names a different review — job \(4656 != 9999\)"
assert_lacks 'case (wv16) a waiver cannot outlive the review it judged' '^prompt-content: WAIVED'
reset_stub

printf '== (wv17) BLOCKER 2: a marker missing a field is MALFORMED, not granted ==\n'
reset_stub
STUB_ANNOUNCE_SHA="$w_head"
STUB_PROMPT="$PROMPT_WITHOUT_PATHS"
STUB_GH_COMMENTS="\001pmcfadin\nroborev-waive: prompt-content-absent head=$w_head job=4656 reason=no base field\n"
run_wrapper "$w_work"
assert_verdict 'case (wv17)' FAIL 1
assert_no_marker_form 'case (wv17)'
assert_says 'case (wv17) a marker missing a field does not match the required form' \
  '^waiver: MALFORMED \(the line begins an authorization of this kind but does not match its required form'
# INVERTED (roborev job 225). This assert USED TO REQUIRE the cause to quote the required form in
# full — i.e. it pinned as a PROPERTY the very thing the spec forbids: "no emitted diagnostic SHALL
# carry any part of the marker — not even its prefix". The detail is interpolated into the `waiver:`
# key, so the block printed a complete, fillable marker beside a live base/head/job, and the comment
# in the checks file two lines from the interpolation asserted it never did. An assert that pins a
# spec violation is worse than no assert: it turns the defect into something a later fix must fight.
assert_says 'case (wv17) and the cause points at --help instead of reproducing the form' \
  "run 'bash scripts/flow/roborev-review.sh --help' for it"
assert_lacks 'case (wv17) the diagnostic carries no part of the marker form' 'roborev-waive'
assert_lacks 'case (wv17) nor the field skeleton that would make one fillable' 'base=<40-hex>'
reset_stub

printf '== (wv18) JOB 24: the waiver loop CLOSES — absence FAIL, waiver, recheck, WAIVED ==\n'
# THE ACCEPTANCE TEST FOR THE WHOLE MECHANISM, not a unit of it. The waiver binds base+head+job and is
# evaluated after that job finishes, but the operator learns the job id FROM the finished run — so before
# `--recheck-job` existed, applying a waiver meant re-running, which enqueued a DIFFERENT job and made the
# fresh waiver instantly STALE. The mechanism was a dead letter: no sequence of actions got a legitimate
# absence past the gate. This case walks the real sequence.
reset_stub
STUB_ANNOUNCE_SHA="$w_head"
STUB_PROMPT="$PROMPT_WITHOUT_PATHS"
run_wrapper "$w_work"
assert_verdict 'case (wv18) step 1: the absence FAILs' FAIL 1
reset_stub
STUB_ANNOUNCE_SHA="$w_head"
STUB_PROMPT="$PROMPT_WITHOUT_PATHS"
# STUB_VERDICT_FIELD='P' because a REAL record always carries a synthesised verdict letter
# (#3564, measured: `roborev show --json` derives it from `reviews.verdict_bool` for every record —
# `P` for a clean review, job 154, `F` for a findings-bearing one, job 162; the `review_jobs` table
# has no verdict column). This case's SUBJECT is the waiver loop, not the findings state, so it is
# fixtured with the payload shape roborev actually emits. Since #3564 a recheck can reach `NONE`
# ONLY from that structured letter — prose cannot establish cleanliness (see fd2) — so without it
# this case would fail on `findings: UNKNOWN` for a reason unrelated to what it tests.
STUB_VERDICT_FIELD='P'
STUB_RECORD_OUTPUT='## Summary\\nNo issues found.'
STUB_GH_COMMENTS="\001pmcfadin\nroborev-waive: prompt-content-absent base=$w_base head=$w_head job=4656 reason=snapshot-delivered; 541812 in / 472576 cached\n"
run_wrapper "$w_work" --recheck-job 4656
assert_verdict 'case (wv18) step 3: the recheck PASSes' PASS 0
assert_says 'case (wv18) the absence is waived for exactly that review' \
  "^prompt-content: WAIVED \(2/2 code census paths absent — authorized by @pmcfadin for base=$w_base head=$w_head job=4656\)\$"
assert_says 'case (wv18) and the waiver key records the whole scope' \
  "^waiver: GRANTED \(author=@pmcfadin base=$w_base head=$w_head job=4656 reason=snapshot-delivered; 541812 in / 472576 cached\)\$"
reset_stub

printf '== (wv19) JOB 24: a recheck DECLARES itself and ENQUEUES NOTHING ==\n'
# A recheck PASS is legitimate but must never be pasteable as evidence of a FRESH review, so the block
# says so in its first key — the way the gate says `MODE: lite`. And the reviewer must not be invoked at
# all: that is checked against the stub's own invocation record, not inferred from the output.
reset_stub
STUB_ANNOUNCE_SHA="$w_head"
STUB_PROMPT="$PROMPT_WITH_PATHS"
STUB_RECORD_OUTPUT='## Summary\\nNo issues found.'
run_wrapper "$w_work" --recheck-job 4656
assert_says 'case (wv19) the block declares the mode and the job' \
  '^MODE: recheck \(job 4656 re-decided from its job record; NO review was enqueued — not evidence of a fresh review\)$'
assert_says 'case (wv19) and names it under its own key' '^recheck-of: 4656$'
assert_says 'case (wv19) roborev-exit does not claim an exit status for a process that never ran' \
  '^roborev-exit: SKIP \(recheck: no reviewer ran in this invocation; job 4656 re-decided from its record\)$'
if grep -q '^review ' "$INVOKED"; then
  bad 'case (wv19) a recheck ENQUEUED a review — the one thing it must never do'
else
  ok 'case (wv19) no review was enqueued (checked against the stub invocation record)'
fi
reset_stub

printf '== (wv20) JOB 24: a recheck RE-ASSERTS from the record, it does not inherit ==\n'
# The original run passing is not evidence for the recheck. With a record whose status is not `done`,
# `review-completed` must FAIL on the recheck exactly as it would on a fresh run.
reset_stub
STUB_ANNOUNCE_SHA="$w_head"
STUB_PROMPT="$PROMPT_WITH_PATHS"
STUB_STATUS="running"
STUB_RECORD_OUTPUT='## Summary\\nNo issues found.'
run_wrapper "$w_work" --recheck-job 4656
assert_verdict 'case (wv20)' FAIL 1
assert_says 'case (wv20) completion is re-asserted from the record, not assumed' \
  "^review-completed: FAIL \(job status 'running' is not done\)\$"
reset_stub

printf '== (wv21) JOB 24: a WHITESPACE-ONLY reason is MALFORMED ==\n'
reset_stub
STUB_ANNOUNCE_SHA="$w_head"
STUB_PROMPT="$PROMPT_WITHOUT_PATHS"
STUB_GH_COMMENTS="\001pmcfadin\nroborev-waive: prompt-content-absent base=$w_base head=$w_head job=4656 reason=   \n"
run_wrapper "$w_work"
assert_verdict 'case (wv21)' FAIL 1
assert_says 'case (wv21) whitespace is not a reason' \
  '^waiver: MALFORMED \(the marker is missing a-non-empty-reason \(the reason is empty or whitespace only\)'
assert_lacks 'case (wv21) and it never grants' '^prompt-content: WAIVED'
reset_stub

printf '== (wv22) JOB 24: a placeholder with TRAILING WHITESPACE is still a placeholder ==\n'
# The classic defeat of a placeholder check: `reason=TODO ` compared before trimming is not equal to
# `todo`, so it passed. The trim now happens BEFORE the judgment.
reset_stub
STUB_ANNOUNCE_SHA="$w_head"
STUB_PROMPT="$PROMPT_WITHOUT_PATHS"
STUB_GH_COMMENTS="\001pmcfadin\nroborev-waive: prompt-content-absent base=$w_base head=$w_head job=4656 reason=TODO   \n"
run_wrapper "$w_work"
assert_verdict 'case (wv22)' FAIL 1
assert_says 'case (wv22) the trimmed value is judged' \
  "^waiver: MALFORMED \(the marker is missing a-substantive-reason \(the reason 'TODO' is a bare placeholder\)"
reset_stub

printf '== (wv23) JOB 24: the documented FIELD ORDER is enforced ==\n'
reset_stub
STUB_ANNOUNCE_SHA="$w_head"
STUB_PROMPT="$PROMPT_WITHOUT_PATHS"
STUB_GH_COMMENTS="\001pmcfadin\nroborev-waive: prompt-content-absent head=$w_head base=$w_base job=4656 reason=fields out of order\n"
run_wrapper "$w_work"
assert_verdict 'case (wv23)' FAIL 1
assert_no_marker_form 'case (wv23)'
assert_says 'case (wv23) a re-ordered marker does not match the required form' \
  '^waiver: MALFORMED \(the line begins an authorization of this kind but does not match its required form'
assert_lacks 'case (wv23) and never grants' '^prompt-content: WAIVED'
reset_stub

printf '== (wv24) JOB 24: field-value BOUNDARIES are enforced ==\n'
# `job=4656x` and a sha with a trailing non-hex character used to survive the per-field `case` extraction
# because nothing bounded the value; one anchored pattern refuses them.
reset_stub
STUB_ANNOUNCE_SHA="$w_head"
STUB_PROMPT="$PROMPT_WITHOUT_PATHS"
STUB_GH_COMMENTS="\001pmcfadin\nroborev-waive: prompt-content-absent base=$w_base head=${w_head}z job=4656x reason=boundary violation\n"
run_wrapper "$w_work"
assert_verdict 'case (wv24)' FAIL 1
assert_no_marker_form 'case (wv24)'
assert_says 'case (wv24) an unbounded field value does not match the required form' \
  '^waiver: MALFORMED \(the line begins an authorization of this kind but does not match its required form'
reset_stub

printf '== (wv24b) ROUND 2: a marker-only comment that is the BARE STEM is MALFORMED, not silent ==\n'
# THE SAME FAIL-QUIET AS (df4d), ON THE OTHER KIND — fixtured for BOTH because the attempt test is
# shared BY CALL, and a fix applied to one kind only would leave the other silent on a truncated
# authorization.
reset_stub
STUB_ANNOUNCE_SHA="$w_head"
STUB_PROMPT="$PROMPT_WITHOUT_PATHS"
STUB_GH_COMMENTS="\001pmcfadin\nroborev-waive: prompt-content-absent"
run_wrapper "$w_work"
assert_verdict 'case (wv24b bare stem)' FAIL 1
assert_no_marker_form 'case (wv24b bare stem)'
assert_says 'case (wv24b) the bare stem is a MALFORMED attempt' \
  '^waiver: MALFORMED \(the line begins an authorization of this kind but does not match its required form'
assert_lacks 'case (wv24b) and not reported as an absent waiver' '^waiver: NONE'
reset_stub
STUB_ANNOUNCE_SHA="$w_head"
STUB_PROMPT="$PROMPT_WITHOUT_PATHS"
STUB_GH_COMMENTS="\001pmcfadin\nroborev-waive: prompt-content-absent\n"
run_wrapper "$w_work"
assert_verdict 'case (wv24b stem plus newline)' FAIL 1
assert_no_marker_form 'case (wv24b stem plus newline)'
assert_says 'case (wv24b stem plus newline) is MALFORMED too' \
  '^waiver: MALFORMED \(the line begins an authorization of this kind but does not match its required form'
assert_lacks 'case (wv24b stem plus newline) not silent' '^waiver: NONE'
reset_stub

printf '== (wv25) JOB 25: a well-formed marker from a NON-ALLOWLISTED author is UNAUTHORIZED ==\n'
# THE HOLE: the author was recorded but never authorized, and this is a PUBLIC repository whose failing
# block PRINTS base, head and job — so any commenter could copy them and pass the merge gate. The state is
# distinct from MALFORMED on purpose: this marker is perfectly well-formed and names exactly this review;
# what disqualifies it is WHO wrote it.
reset_stub
STUB_ANNOUNCE_SHA="$w_head"
STUB_PROMPT="$PROMPT_WITHOUT_PATHS"
STUB_GH_COMMENTS="\001drive-by-contributor\nroborev-waive: prompt-content-absent base=$w_base head=$w_head job=4656 reason=copied from the public failing block\n"
run_wrapper "$w_work"
assert_verdict 'case (wv25)' FAIL 1
assert_says 'case (wv25) a stranger cannot grant a waiver' \
  "^waiver: UNAUTHORIZED \(the marker is well-formed and names this review, but its author '@drive-by-contributor' is not on the waiver allowlist"
assert_says 'case (wv25) the cause says why authorship is the only separator here' \
  'this is a public repository'
assert_says 'case (wv25) the absence FAIL stands' \
  '^prompt-content: FAIL \(2/2 code census paths absent from the prompt\)$'
assert_lacks 'case (wv25) and it never grants' '^prompt-content: WAIVED'
reset_stub

printf '== (wv26) JOB 25: an allowlisted author with the SAME marker DOES grant ==\n'
# The positive control for wv25: the ONLY difference is the author, so the case pins that the allowlist is
# what decided it — not some incidental property of the marker.
reset_stub
STUB_ANNOUNCE_SHA="$w_head"
STUB_PROMPT="$PROMPT_WITHOUT_PATHS"
STUB_GH_COMMENTS="\001pmcfadin\nroborev-waive: prompt-content-absent base=$w_base head=$w_head job=4656 reason=copied from the public failing block\n"
run_wrapper "$w_work"
assert_verdict 'case (wv26)' PASS 0
assert_says 'case (wv26) the same marker from an allowlisted author grants' '^prompt-content: WAIVED'
reset_stub

printf '== (wv27) JOB 25: an UNAUTHORIZED marker does not shadow an allowlisted one ==\n'
# Ordering matters: a stranger commenting first must not make the real authorization unreachable.
reset_stub
STUB_ANNOUNCE_SHA="$w_head"
STUB_PROMPT="$PROMPT_WITHOUT_PATHS"
STUB_GH_COMMENTS="\001drive-by-contributor\nroborev-waive: prompt-content-absent base=$w_base head=$w_head job=4656 reason=stranger first\n\001pmcfadin\nroborev-waive: prompt-content-absent base=$w_base head=$w_head job=4656 reason=authorized after review of the token accounting\n"
run_wrapper "$w_work"
assert_verdict 'case (wv27)' PASS 0
assert_says 'case (wv27) the allowlisted grant is still found' \
  "^waiver: GRANTED \(author=@pmcfadin base=$w_base head=$w_head job=4656 reason=authorized after review of the token accounting\)\$"
reset_stub

printf '== (wv28) JOB 25: the record review text is read under EITHER field name ==\n'
# `roborev-job-facts.py` documented `verdict_text` and read only `output`, so a payload using the other
# spelling produced an EMPTY transcript and spuriously failed review-completed/findings on a recheck. Both
# payload shapes are fixtured: the field on the nested JOB row, and the field on the REVIEW row.
reset_stub
STUB_ANNOUNCE_SHA="$w_head"
STUB_PROMPT="$PROMPT_WITH_PATHS"
STUB_PROMPT='### Combined Diff\n\ndiff --git a/alpha.rs b/alpha.rs\n@@ x @@\ndiff --git a/beta.rs b/beta.rs\n@@ y @@'
STUB_RECORD_OUTPUT_FIELD=verdict_text
STUB_VERDICT_FIELD='P'   # real records always carry the synthesised letter (#3564) — see wv18
STUB_RECORD_OUTPUT='## Summary\\nNo issues found.'
run_wrapper "$w_work" --recheck-job 4656
assert_verdict 'case (wv28) verdict_text on the job row' PASS 0
assert_says 'case (wv28) completion is re-asserted from verdict_text' '^review-completed: PASS$'
reset_stub
STUB_ANNOUNCE_SHA="$w_head"
STUB_PROMPT="$PROMPT_WITH_PATHS"
STUB_SHOW_JSON=nested
STUB_PROMPT='### Combined Diff\n\ndiff --git a/alpha.rs b/alpha.rs\n@@ x @@\ndiff --git a/beta.rs b/beta.rs\n@@ y @@'
STUB_RECORD_OUTPUT_FIELD=verdict_text
STUB_VERDICT_FIELD='P'   # real records always carry the synthesised letter (#3564) — see wv18
STUB_RECORD_OUTPUT='## Summary\\nNo issues found.'
run_wrapper "$w_work" --recheck-job 4656
assert_verdict 'case (wv28b) verdict_text on the review row' PASS 0
assert_says 'case (wv28b) completion is re-asserted from the review row field' '^review-completed: PASS$'
reset_stub

printf '== (wv29) JOB 26: a FORGED author line inside a stranger comment does not grant ==\n'
# THE DEFEAT OF LAST ROUND'S ALLOWLIST, and the fourth member of one family on this issue: CONTROL AND
# DATA MUST NOT SHARE A CHANNEL WHEN THE DATA IS ATTACKER-CONTROLLED. Comments used to be flattened into
# one stream where an author record was a leading-SOH line and the body followed verbatim, so an
# unauthorized commenter could put an allowlisted login on its own SOH line INSIDE their own body and
# have the next marker attributed to it. The fixture is that exploit, passed as raw JSON so the forged
# line is unambiguously BODY CONTENT of a stranger's comment — the harness cannot turn it into a second
# comment, which would test the fixture layer instead of the code.
reset_stub
STUB_ANNOUNCE_SHA="$w_head"
STUB_PROMPT="$PROMPT_WITHOUT_PATHS"
STUB_GH_COMMENTS_JSON=$(FORGE_BASE="$w_base" FORGE_HEAD="$w_head" python3 -c '
import json, os
marker = ("roborev-waive: prompt-content-absent base=%s head=%s job=4656 reason=forged an author line"
          % (os.environ["FORGE_BASE"], os.environ["FORGE_HEAD"]))
body = "\u0001pmcfadin\n" + marker
print(json.dumps({"comments": [{"author": {"login": "drive-by-contributor"}, "body": body}]}))
')
run_wrapper "$w_work"
assert_verdict 'case (wv29)' FAIL 1
# TWO LAYERS NOW REFUSE THIS, and the outer one fires first: the forged author line is a SECOND nonblank
# line, so the comment is not an authorization at all (job 29). Even if it were marker-only, the author would
# still be off the allowlist (job 25) — (wv25) pins that half with a marker-only fixture.
assert_says 'case (wv29) a forged author line is not an authorization at all' \
  '^waiver: NONE \(no waiver comment for this review: the marker must be the SOLE NONBLANK CONTENT'
assert_lacks 'case (wv29) a forged author line cannot grant' '^prompt-content: WAIVED'
assert_lacks 'case (wv29) and the forged login is never credited' 'authorized by @pmcfadin'
assert_says 'case (wv29) the absence FAIL stands' \
  '^prompt-content: FAIL \(2/2 code census paths absent from the prompt\)$'
reset_stub

printf '== (wv30) JOB 26: a body line that merely LOOKS like a delimiter is inert ==\n'
# The complement: with no delimiter left to interpret, a bare login line in a body is just prose. The
# case pins that the fix is "no in-band channel", not "a rarer delimiter" — the latter would still grant
# for some spelling.
reset_stub
STUB_ANNOUNCE_SHA="$w_head"
STUB_PROMPT="$PROMPT_WITHOUT_PATHS"
STUB_GH_COMMENTS="\001drive-by-contributor\npmcfadin\nroborev-waive: prompt-content-absent base=$w_base head=$w_head job=4656 reason=bare login line above the marker\n"
run_wrapper "$w_work"
assert_verdict 'case (wv30)' FAIL 1
assert_says 'case (wv30) a bare login line is extra content, so the comment is not an authorization' \
  '^waiver: NONE \(no waiver comment for this review: the marker must be the SOLE NONBLANK CONTENT'
reset_stub

printf '== (wv31) JOB 26/27: an ABSENT scanner is UNAVAILABLE, never a text fallback ==\n'
# A waiver is never decided from a flattened text stream, so a missing scanner fails closed instead of
# degrading to line parsing — the shape that produced this whole family.
#
# THE FIXTURE SUBSTITUTES THE FILE, NOT THE PATH (job 27). There is no environment override to point the
# wrapper elsewhere — that override was itself the hole: the constrained party must not choose its own
# enforcer. So this case runs the wrapper from a SCRATCH COPY of `scripts/flow/` with the scanner omitted,
# which is a state a real checkout can be in and needs no seam in production code.
reset_stub
noscan="$tmp/flow-without-scanner"
mkdir -p "$noscan"
cp "$WRAPPER_REAL" "$noscan/roborev-review.sh"
cp "$SCRIPT_DIR/../flow/roborev-review-oracles.sh" "$noscan/"
cp "$SCRIPT_DIR/../flow/roborev-review-checks.sh" "$noscan/"
cp "$SCRIPT_DIR/../flow/roborev-job-facts.py" "$noscan/"
# ...and deliberately NOT roborev-waiver-scan.py.
STUB_ANNOUNCE_SHA="$w_head"
STUB_PROMPT="$PROMPT_WITHOUT_PATHS"
STUB_GH_COMMENTS="\001pmcfadin\nroborev-waive: prompt-content-absent base=$w_base head=$w_head job=4656 reason=would otherwise grant\n"
CASE_N=$((CASE_N + 1))
OUT="$tmp/out-$CASE_N.txt"
INVOKED="$tmp/invoked-$CASE_N.txt"
: >"$INVOKED"
STUB_GIT_REF=$(range_ref "$w_work")
STUB_INVOKED="$INVOKED" PATH="$stubbin:$PATH" HOME="$FIXTURE_HOME" \
  TMPDIR="${WRAPPER_TMPDIR:-$WRAPPER_TMP}" \
  bash "$noscan/roborev-review.sh" --repo "$w_work" --agent codex --model gpt-5.6-sol \
  --log "$tmp/transcript-$CASE_N.txt" >"$OUT" 2>&1
RC=$?
assert_verdict 'case (wv31)' FAIL 1
assert_says 'case (wv31) an unusable scanner is UNAVAILABLE and fails closed' \
  '^waiver: UNAVAILABLE \(the structured waiver scanner is unusable'
assert_says 'case (wv31) and it says a waiver is never decided from a text stream' \
  'NEVER decided from a flattened text stream'
# THE LOAD-BEARING CONTROL FOR THE KEYWORD REDACTION'S WORD BOUNDARY (roborev job 230). This cause names
# the SCANNER'S OWN FILE, whose name embeds `roborev-waive` as a substring, and the operator has to read
# that path to fix a fail-closed UNAVAILABLE. A blanket substring redaction mangles it into an
# unactionable diagnostic, so the denylist fires only where the keyword is not continued by a letter —
# and this is the assert that would catch it being loosened back. (`assert_no_marker_form` is
# deliberately NOT attached to this case: its grep for the bare keyword is unanchored, i.e. stricter
# than the renderer, and it would red on the very path this case exists to preserve.)
assert_says 'case (wv31) the scanner PATH survives the keyword redaction, so the cause can be acted on' \
  'tool: [^ ]*roborev-waiver-scan\.py'
assert_lacks 'case (wv31) and nothing in this cause was redacted at all' 'authorization-keyword-redacted'
assert_lacks 'case (wv31) no grant without the scanner' '^prompt-content: WAIVED'
reset_stub

printf '== (wv32) JOB 27: a hostile WAIVER_SCAN_TOOL in the environment is IGNORED ==\n'
# THE BYPASS THIS CLOSES: the enforcer path was env-settable, so an invoker could point it at a script
# printing `state=granted` and turn an absent prompt into an overall PASS with no authorized comment at
# all. That is the allowlist argument one level out — THE CONSTRAINED PARTY MUST NOT CHOOSE ITS OWN
# ENFORCER — so the path is now resolved from the wrapper's own directory and the variable is inert.
reset_stub
forge_scanner="$tmp/forged-scanner.py"
cat >"$forge_scanner" <<'FORGED'
#!/usr/bin/env python3
print("state=granted")
print("author=attacker")
print("scope=base=deadbee head=deadbee job=1")
print("reason=fabricated by a hostile scanner")
print("detail=")
FORGED
chmod +x "$forge_scanner"
STUB_ANNOUNCE_SHA="$w_head"
STUB_PROMPT="$PROMPT_WITHOUT_PATHS"
STUB_GH_COMMENTS=""
WAIVER_SCAN_TOOL="$forge_scanner" run_wrapper "$w_work"
assert_verdict 'case (wv32)' FAIL 1
assert_says 'case (wv32) the absence FAIL stands, with no waiver found' \
  '^prompt-content: FAIL \(2/2 code census paths absent from the prompt\)$'
assert_says 'case (wv32) and the waiver state comes from the real scanner' \
  '^waiver: NONE \(no waiver comment for this review: the marker must be the SOLE NONBLANK CONTENT'
assert_lacks 'case (wv32) a hostile scanner cannot fabricate a grant' '^prompt-content: WAIVED'
assert_lacks 'case (wv32) and its fabricated author is never credited' 'attacker'
reset_stub

printf '== (wv33) JOB 28/29 REGRESSION: a FENCED marker from an allowlisted author does NOT grant ==\n'
# KEPT AS A REGRESSION ACROSS TWO DESIGNS (jobs 28 and 29). It used to grant because a fence preserves
# column zero; the fence machine then excluded it; the sole-content rule now excludes it for a SIMPLER
# reason — a fenced marker is never the only nonblank line. The case is pinned from both designs so a future
# edit cannot reopen it by deleting the newer rule.
reset_stub
STUB_ANNOUNCE_SHA="$w_head"
STUB_PROMPT="$PROMPT_WITHOUT_PATHS"
STUB_GH_COMMENTS_JSON=$(FENCE_BASE="$w_base" FENCE_HEAD="$w_head" python3 -c '
import json, os
marker = ("roborev-waive: prompt-content-absent base=%s head=%s job=4656 reason=documenting the form"
          % (os.environ["FENCE_BASE"], os.environ["FENCE_HEAD"]))
body = "Here is how the waiver marker looks:\n\n```\n" + marker + "\n```\n\nPost it as a top-level comment."
print(json.dumps({"comments": [{"author": {"login": "pmcfadin"}, "body": body}]}))
')
run_wrapper "$w_work"
assert_verdict 'case (wv33)' FAIL 1
assert_says 'case (wv33) a fenced marker is data, not an authorization' \
  '^waiver: NONE \(no waiver comment for this review: the marker must be the SOLE NONBLANK CONTENT'
assert_lacks 'case (wv33) documenting the syntax must not grant' '^prompt-content: WAIVED'
assert_says 'case (wv33) the absence FAIL stands' \
  '^prompt-content: FAIL \(2/2 code census paths absent from the prompt\)$'
reset_stub

printf '== (wv33b) JOB 28: the SAME marker unfenced, same author, still grants (positive control) ==\n'
# The control that makes wv33 meaningful: only the fence differs, so the case pins that the FENCE decided
# it — not the author, the scope, the reason or some incidental fixture difference.
reset_stub
STUB_ANNOUNCE_SHA="$w_head"
STUB_PROMPT="$PROMPT_WITHOUT_PATHS"
STUB_GH_COMMENTS="\001pmcfadin\nroborev-waive: prompt-content-absent base=$w_base head=$w_head job=4656 reason=documenting the form\n"
run_wrapper "$w_work"
assert_verdict 'case (wv33b)' PASS 0
assert_says 'case (wv33b) the unfenced marker grants' \
  "^waiver: GRANTED \(author=@pmcfadin base=$w_base head=$w_head job=4656 reason=documenting the form\)\$"
reset_stub

printf '== (wv33c) JOB 29: an HTML <pre> block and a `bash`-info-string fence are BOTH inert ==\n'
# The two contexts the fence machine got wrong (job 29 finding): ````bash` inside a fence is CONTENT, not a
# closing delimiter, so fence state desynchronised and a later marker granted; and HTML <pre>/<code> was
# never handled at all. Under the sole-content rule neither needs handling — both are extra content.
reset_stub
STUB_ANNOUNCE_SHA="$w_head"
STUB_PROMPT="$PROMPT_WITHOUT_PATHS"
STUB_GH_COMMENTS_JSON=$(SOLE_BASE="$w_base" SOLE_HEAD="$w_head" python3 -c '
import json, os
marker = ("roborev-waive: prompt-content-absent base=%s head=%s job=4656 reason=documenting the form"
          % (os.environ["SOLE_BASE"], os.environ["SOLE_HEAD"]))
fence_confusion = "```\n```bash\n" + marker + "\n```"
html_pre = "<pre>\n" + marker + "\n</pre>"
print(json.dumps({"comments": [
    {"author": {"login": "pmcfadin"}, "body": fence_confusion},
    {"author": {"login": "pmcfadin"}, "body": html_pre}]}))
')
run_wrapper "$w_work"
assert_verdict 'case (wv33c)' FAIL 1
assert_says 'case (wv33c) neither preformatted context is an authorization' \
  '^waiver: NONE \(no waiver comment for this review: the marker must be the SOLE NONBLANK CONTENT'
assert_lacks 'case (wv33c) and neither grants' '^prompt-content: WAIVED'
reset_stub

printf '== #3564: `findings:` MUST FAIL THE VERDICT ON ITS OWN, IN EVERY MODE ==\n'
# THE DEFECT (#3564, measured on #3473's round-3 recovery, job 160): `--recheck-job` emitted
#     findings:     PRESENT (3)
#     roborev-exit: SKIP (recheck: no reviewer ran ...)
#     RESULT:       PASS
# a FALSE PASS IN A MERGE GATE. `PRESENT` is in the verdict grammar's non-failing set, so the only
# thing failing a findings-bearing run was the NEIGHBOURING key `roborev-exit: FINDINGS (exit 1)`.
# On a recheck no reviewer process runs, `roborev-exit` is legitimately `SKIP`, and removing the
# failing signal removed the only thing failing the run.
#
# BOTH DIRECTIONS ARE PINNED, and that is a requirement of the fix rather than thoroughness: a
# one-direction test cannot distinguish a corrected verdict scan from one that fails EVERYTHING,
# and `--recheck-job` is the ONLY path the #3312 absence waiver can travel, so a verdict scan that
# over-fails would break the break-glass instead of the false PASS.
FINDINGS_TEXT='## Findings\n- **Severity**: High\nProblem: the first one.\n- **Severity**: Medium\nProblem: the second one.\n## Summary\n2 findings.'
CLEAN_TEXT='## Summary\nNo issues found.'
# A prompt carrying THIS fixture's OWN census paths (`two-code-commits` = alpha.rs + beta.rs), so
# `prompt-content:` can PASS. The shared PROMPT_WITH_PATHS names main.rs/README.md/NOTES.md — the
# `mixed` fixture's paths — which is why the waiver cases above either waive the absence or assert
# only individual keys. A positive control needs a prompt that genuinely matches its census, not a
# waiver: a PASS bought with a waiver could not show that findings NONE is what permitted it.
PROMPT_WITH_W_PATHS='Review the following change.\ndiff --git a/alpha.rs b/alpha.rs\n@@ fn alpha() {} @@\ndiff --git a/beta.rs b/beta.rs\n@@ fn beta() {} @@'

printf '== (fd1) #3564: a recheck of a job whose record CARRIES FINDINGS FAILs ==\n'
# The exact measured signature, asserted as a signature: PRESENT beside SKIP must not be a PASS.
# EVERY OTHER KEY IS MADE TO PASS (hence the census-matching prompt), so the findings are the SOLE
# cause of the FAIL. That is what makes this a test of the defect rather than of the suite: with any
# other key failing, the run FAILs for a reason that was never in question and the case would go on
# passing if the fix were reverted.
reset_stub
STUB_ANNOUNCE_SHA="$w_head"
STUB_PROMPT="$PROMPT_WITH_W_PATHS"
STUB_VERDICT_FIELD='F'
STUB_RECORD_OUTPUT="$FINDINGS_TEXT"
run_wrapper "$w_work" --recheck-job 4656
assert_verdict 'case (fd1)' FAIL 1
assert_says 'case (fd1) the findings state is re-asserted from the record' '^findings: PRESENT \(2\)$'
assert_says 'case (fd1) roborev-exit stays truthful about a process that never ran' \
  '^roborev-exit: SKIP \(recheck: no reviewer ran in this invocation; job 4656 re-decided from its record\)$'
# THE ISOLATION CLAIM, PINNED RATHER THAN ASSERTED IN PROSE (C audit, low). Reaching the terminal gate
# proves only that no key failed the GRAMMAR scan; it does not prove the other keys affirmatively PASSed,
# which is what makes findings the SOLE cause. So the two keys a findings-bearing fixture is most likely
# to disturb are asserted positively — without this, the case would keep passing for the wrong reason if
# a future fixture change broke one of them.
assert_says 'case (fd1) the reviewer-diff delivery key affirmatively PASSed (findings are the SOLE cause)' \
  '^prompt-content: PASS \(2/2 code census paths present\)$'
assert_says 'case (fd1) and completion was re-asserted from the record' '^review-completed: PASS$'
assert_says 'case (fd1) and findings fails the verdict on its OWN terms, naming what it read' \
  "ERROR: findings: this run would have PASSED while 'findings:' reads 'PRESENT \(2\)'"
assert_lacks 'case (fd1) THE #3473 SIGNATURE IS UNREACHABLE: no PASS beside PRESENT' '^RESULT: PASS$'
reset_stub

printf '== (fd2) #3564: prose CANNOT establish cleanliness — no structured verdict is UNKNOWN ==\n'
# ROUND 2 OF REVIEW KILLED THE RECONSTRUCTION THIS CASE USED TO ASSERT. It originally required a
# clean-LOOKING record with NO structured verdict to PASS, i.e. it treated "no severity marker found"
# as affirmative cleanliness. Two rounds each found a review SHAPE that defeats that: a HEADERLESS
# findings review (fd9), and a findings BLOCK with no recognised marker (fd10). The class does not
# close, and cannot: `review-completed` accepts a bare `## Summary` heading as a completed review, so
# a findings review whose findings are prose is INDISTINGUISHABLE from a clean one (fd11) — and a real
# clean review's text is `No issues found.\n\nSummary: ...` with no `Findings` heading either.
#
# So the direction is asymmetric and permanent: a marker in a findings block is positive evidence OF
# findings, while its absence is NOT evidence of cleanliness. `NONE` is reachable only from the
# structured `verdict` letter (fd4). THE BREAK-GLASS IS UNHARMED, and that is measured rather than
# argued: `roborev show --json` synthesises that letter from `reviews.verdict_bool` for EVERY observed
# record, so a real recheck of a clean job takes the structured path — see fd4.
reset_stub
STUB_ANNOUNCE_SHA="$w_head"
STUB_PROMPT="$PROMPT_WITH_W_PATHS"
STUB_RECORD_OUTPUT="$CLEAN_TEXT"
run_wrapper "$w_work" --recheck-job 4656
assert_verdict 'case (fd2) a clean-LOOKING record with no structured verdict does NOT pass' FAIL 1
assert_says 'case (fd2) the state is UNKNOWN — prose cannot establish cleanliness' '^findings: UNKNOWN$'
assert_says 'case (fd2) and the cause says so, and flags the payload shape as unexpected' \
  'prose can therefore never establish CLEANLINESS'
assert_lacks 'case (fd2) NONE is never derived from an absent marker' '^findings: NONE$'
reset_stub

printf '== (fd3) #3564: a recheck with NO structured verdict reads findings FROM THE RECORD TEXT ==\n'
# THE SECOND HALF OF THE FIX, and it is not cosmetic. `findings:` used to fall through to a branch
# keyed on the REVIEWER'S EXIT CODE when the record carried no `verdict` field — and a recheck has
# no reviewer, so `roborev-exit: SKIP` matched neither arm and the key read UNKNOWN. Harmless while
# nothing depended on `findings` alone; once it gates the verdict, UNKNOWN on every such recheck
# would have false-FAILed the clean ones (fd2's shape) and, in this shape, still not named the
# findings. The record's review text IS the transcript in this mode, so it is the right oracle.
reset_stub
STUB_ANNOUNCE_SHA="$w_head"
STUB_PROMPT="$PROMPT_WITH_W_PATHS"
STUB_RECORD_OUTPUT="$FINDINGS_TEXT"
run_wrapper "$w_work" --recheck-job 4656
assert_verdict 'case (fd3)' FAIL 1
assert_says 'case (fd3) findings come from the record text, not from the absent exit code' \
  '^findings: PRESENT \(2\)$'
assert_lacks 'case (fd3) and never degrades to UNKNOWN' '^findings: UNKNOWN'
reset_stub

printf '== (fd4) #3564: a recheck of a job whose record verdict is affirmatively CLEAN PASSes ==\n'
# THE POSITIVE CONTROL, and after round 2 the ONLY one: `NONE` is reachable only from the structured
# verdict letter. Without this case the suite could not distinguish the fix from a scan that fails
# every recheck — which would take the only route an authorized absence waiver has (#3312 job 24).
#
# AND IT IS THE REALISTIC SHAPE, measured rather than assumed: `roborev show --json` SYNTHESISES this
# letter from the `reviews.verdict_bool` column for every observed record — `P` for a clean review
# (job 154, verdict_bool=1) and `F` for a findings-bearing one (job 162, verdict_bool=0), while the
# `review_jobs` table has no verdict column at all. So THIS is the path a real clean recheck takes,
# and fd2's verdict-less payload is the defensive one.
reset_stub
STUB_ANNOUNCE_SHA="$w_head"
STUB_PROMPT="$PROMPT_WITH_W_PATHS"
STUB_VERDICT_FIELD='P'
STUB_RECORD_OUTPUT="$CLEAN_TEXT"
run_wrapper "$w_work" --recheck-job 4656
assert_verdict 'case (fd4)' PASS 0
assert_says 'case (fd4) the structured clean verdict reads NONE' '^findings: NONE$'
reset_stub

printf '== (fd5) #3564: AN AUTHORIZED ABSENCE WAIVER DOES NOT EXCUSE FINDINGS ==\n'
# THE SHARPEST CASE IN THIS GROUP. `--recheck-job` is the only path a waiver travels, so before the
# fix the one code path an authorized waiver must use was the path that silently dropped a findings
# failure — letting a waiver scoped to `prompt-content` ABSENCE (#3312) excuse findings NO HUMAN
# AUTHORIZED. The waiver must still do its own job (prompt-content: WAIVED) and the run must still
# FAIL on the findings.
reset_stub
STUB_ANNOUNCE_SHA="$w_head"
STUB_PROMPT="$PROMPT_WITHOUT_PATHS"
STUB_VERDICT_FIELD='F'
STUB_RECORD_OUTPUT="$FINDINGS_TEXT"
STUB_GH_COMMENTS="\001pmcfadin\nroborev-waive: prompt-content-absent base=$w_base head=$w_head job=4656 reason=snapshot-delivered; 541812 in / 472576 cached\n"
run_wrapper "$w_work" --recheck-job 4656
assert_verdict 'case (fd5)' FAIL 1
assert_says 'case (fd5) the waiver still excuses exactly what it was authorized for' \
  '^prompt-content: WAIVED \(2/2 code census paths absent'
assert_says 'case (fd5) but the findings are NOT waived' '^findings: PRESENT \(2\)$'
assert_says 'case (fd5) and the diagnostic says the requirement is unwaivable' \
  'NOT waivable in any mode: the absence waiver excuses prompt-content absence only'
assert_lacks 'case (fd5) a waiver can never carry a findings-bearing recheck to a PASS' '^RESULT: PASS$'
reset_stub

printf '== (fd6) #3564: the FRESH-review path is unchanged (regression control) ==\n'
# Rounds 1 and 2 on #3473 produced the CORRECT verdict, so the fix must not have been bought by
# altering the path that already worked: a fresh review reporting findings still fails, and still
# fails under `roborev-exit: FINDINGS (exit 1)` — the honest statement about a reviewer that DID run.
reset_stub
STUB_ANNOUNCE_SHA="$w_head"
STUB_PROMPT="$PROMPT_WITH_W_PATHS"
STUB_VERDICT=$'## Findings\n- **Severity**: High\nProblem: the first one.\n## Summary\n1 finding.'
STUB_REVIEW_RC=1
run_wrapper "$w_work"
assert_verdict 'case (fd6)' FAIL 1
assert_says 'case (fd6) a reviewer that RAN reports its exit honestly' '^roborev-exit: FINDINGS \(exit 1\)$'
assert_says 'case (fd6) and findings are reported present' '^findings: PRESENT'
reset_stub

printf '== (fd8) #3564: a recheck of a record with NO review text cannot report NONE ==\n'
# An EMPTY transcript measured nothing, so "no severity markers found" is true and meaningless — and
# NONE is now the PERMISSIVE value. `review-completed` already FAILs an empty transcript, so this is
# defence in depth; it is asserted because a guard that holds only because a NEIGHBOURING check fails
# first is the exact coupling this issue removed one key over.
reset_stub
STUB_ANNOUNCE_SHA="$w_head"
STUB_PROMPT="$PROMPT_WITH_W_PATHS"
STUB_RECORD_OUTPUT=''
run_wrapper "$w_work" --recheck-job 4656
assert_verdict 'case (fd8)' FAIL 1
assert_says 'case (fd8) an absent measurement is UNKNOWN, never NONE' '^findings: UNKNOWN$'
assert_says 'case (fd8) and the recheck says the record carried no review text' \
  'NOTICE: recheck: the job record for .4656. carries no review text'
reset_stub

printf '== (fd9) #3564 roborev r1 High: a HEADERLESS findings recheck cannot read NONE ==\n'
# THE FIX'S OWN DEFECT CLASS, ONE LAYER DOWN. `review-completed`'s allow-list deliberately ACCEPTS a
# findings review with NO `Findings` heading (a bare `**Severity**:` line, `[High]`, `Medium:`) —
# it was widened to stop false-FAILing genuine reviews from agents that emit that shape. But the
# findings-BLOCK extraction needs a heading, so for such a transcript the block is EMPTY, and the
# recheck fallback read 0 markers as an AFFIRMATIVE `NONE` and PASSED a findings-bearing recheck.
# THE SHIPPED RULE IS STRONGER THAN THIS CASE'S FIRST FIX: an intermediate version made `NONE` require
# zero markers across the WHOLE transcript, and round 2 defeated that too (fd10/fd11), so prose can now
# never say `NONE` at all — only the structured verdict letter can.
reset_stub
STUB_ANNOUNCE_SHA="$w_head"
STUB_PROMPT="$PROMPT_WITH_W_PATHS"
STUB_RECORD_OUTPUT='- **Severity**: High\n- Problem: a headerless finding, with no Findings heading anywhere.\n## Summary\n1 finding.'
run_wrapper "$w_work" --recheck-job 4656
assert_verdict 'case (fd9) a headerless findings recheck does NOT pass' FAIL 1
assert_says 'case (fd9) the state is UNKNOWN, not NONE' '^findings: UNKNOWN$'
assert_says 'case (fd9) and the cause names the ambiguity rather than claiming cleanliness' \
  'prose can therefore never establish CLEANLINESS'
assert_lacks 'case (fd9) NONE is never reported for a marker-bearing transcript' '^findings: NONE$'
reset_stub

printf '== (fd9b) #3564: the bracket and `Medium:` headerless shapes are covered too ==\n'
# The allow-list names three headerless shapes; a fix that only recognised `**Severity**:` would leave
# the other two reaching NONE. Asserted per shape rather than trusting one to stand for the family.
for _fd9_shape in '[High] a bracketed headerless finding.' 'Medium: a labelled headerless finding.'; do
  reset_stub
  STUB_ANNOUNCE_SHA="$w_head"
  STUB_PROMPT="$PROMPT_WITH_W_PATHS"
  STUB_RECORD_OUTPUT="$_fd9_shape\n## Summary\n1 finding."
  run_wrapper "$w_work" --recheck-job 4656
  assert_verdict "case (fd9b) '$_fd9_shape' does NOT pass" FAIL 1
  assert_says "case (fd9b) '$_fd9_shape' is UNKNOWN, not NONE" '^findings: UNKNOWN$'
  reset_stub
done

printf '== (fd10) #3564 roborev r2 High: a MARKERLESS findings BLOCK cannot read NONE ==\n'
# ROUND 2'S FINDING. A `## Findings` block whose findings carry NO recognised severity marker leaves
# the block NON-EMPTY but the marker count at zero — so the intermediate fix (which required only
# "zero markers anywhere") still read it as affirmative NONE and passed a findings-bearing recheck.
reset_stub
STUB_ANNOUNCE_SHA="$w_head"
STUB_PROMPT="$PROMPT_WITH_W_PATHS"
STUB_RECORD_OUTPUT='## Findings\n- The reconstruction is wrong, stated without any severity label.\n## Summary\n1 finding.'
run_wrapper "$w_work" --recheck-job 4656
assert_verdict 'case (fd10) a markerless findings block does NOT pass' FAIL 1
assert_says 'case (fd10) the state is UNKNOWN, not NONE' '^findings: UNKNOWN$'
assert_lacks 'case (fd10) NONE is never reported for a findings-bearing record' '^findings: NONE$'
reset_stub

printf '== (fd11) #3564: prose findings under a Summary heading ALONE — the unclosable shape ==\n'
# THE CASE THAT JUSTIFIES REMOVING THE RECONSTRUCTION RATHER THAN PATCHING IT A THIRD TIME.
# `review-completed` accepts a `## Summary` heading ALONE as a completed review. So this transcript —
# prose findings, no `Findings` heading, no severity marker — is a VALID completed review that reports
# findings, and it is textually indistinguishable from a clean review (whose real text is
# `No issues found.\n\nSummary: ...`, also with no Findings heading). No recogniser over prose can
# separate them, which is why `NONE` now requires the structured verdict letter instead.
reset_stub
STUB_ANNOUNCE_SHA="$w_head"
STUB_PROMPT="$PROMPT_WITH_W_PATHS"
STUB_RECORD_OUTPUT='## Summary\nThe reconstruction is wrong and should be removed. One issue.'
run_wrapper "$w_work" --recheck-job 4656
assert_verdict 'case (fd11) a Summary-only findings review does NOT pass' FAIL 1
assert_says 'case (fd11) the state is UNKNOWN, not NONE' '^findings: UNKNOWN$'
assert_says 'case (fd11) review-completed still accepts it AS a completed review' '^review-completed: PASS$'
reset_stub

printf '== (fd7) #3564 structural: the findings gate is TOKEN-EXACT and not keyed on a neighbour ==\n'
# Behavioural cases only cover the shapes someone already thought of. Two properties of the FIX are
# asserted against the shipped wrapper, because both are exactly what a later "simplification" would
# undo: the value is reduced to its VERDICT TOKEN and compared to `NONE` EXACTLY (a `NONE*` prefix
# glob would accept a `NONE-BUT-UNMEASURED` state, which is the closure checking a spelling rather
# than a state — the #3229 lesson), and the comparison is on `FINDINGS` itself rather than on
# `ROBOREV_EXIT`, which is the delegation #3564 removed.
if grep -qF '[ "${FINDINGS%% *}" != NONE ]' "$WRAPPER_REAL"; then
  ok 'structural: the terminal gate reduces findings to its verdict token and requires NONE exactly'
else
  bad 'structural: the terminal findings gate is not a token-exact `!= NONE` test on FINDINGS — a prefix glob or a neighbour-keyed test reopens #3564'
fi
if grep -qF 'This requirement is NOT waivable in any mode' "$WRAPPER_REAL"; then
  ok 'structural: the gate records IN CODE that it is unwaivable (the #3312 waiver is absence-only)'
else
  bad 'structural: the findings gate no longer states that it is unwaivable — the next reader will re-add a WAIVED branch'
fi
reset_stub

# =============================================================================================
# (df*) #3626: "ROBOREV CLEAN" MEANS NO UNADDRESSED FINDINGS — THE LEAD-AUTHORIZED DEFERRAL
# =============================================================================================
# THE DEADLOCK THIS FAMILY EXISTS FOR, measured on PR #3572 job 262: two findings, ZERO new, both
# already filed (#3602, #3613) and both already LEAD-DEFERRED, 5.9M input tokens, every deterministic
# key PASS — and `findings: PRESENT (2)` with `RESULT: FAIL`. Since #3586 that requirement is correct
# and NOT waivable, but roborev re-reports a deferred finding on EVERY later round, so the doctrine
# rule "any non-PASS terminal RESULT is a blocked merge" blocked that merge FOREVER. The lane the fix
# protects is the one that behaved correctly: it refused to arm --auto over a FAIL, refused to
# manufacture a green, and asked instead. A rule that punishes the correct behaviour will not survive
# contact.
#
# WHAT IS PINNED HERE: a deferral is granted ONLY by a marker on the ABSENCE WAIVER'S CHANNEL (sole
# nonblank content of a top-level PR comment, allowlisted author, structured author association),
# ONLY on `--recheck-job`, ONLY over an affirmatively measured `PRESENT (n)`, and only when the
# authorized count EQUALS the observed count with every named issue RETRIEVABLE from GitHub — asked
# three-valued, so "the issue does not exist" and "this box could not ask" are separate non-granting
# states and neither reads as verified. It reports a DISTINCT `DEFERRED` token, NEVER `NONE`. Every
# refusal state leaves the FAIL in place under its own name, and the deferral cannot touch any other
# verdict.
#
# NO PR-BODY LINK IS REQUIRED, AND THE CASES THAT PINNED ONE ARE GONE (#3626, lead ruling). An earlier
# revision also demanded a visible local `#<N>` in the PR BODY, and (df8, df8b–df8f, df9b) pinned its
# Markdown recognisers. The leg was DELETED, not patched: a PR body is editable at any time by anyone
# with write access with NO per-edit attribution, while a top-level comment is permanent and
# attributable — so the body was the WEAKER artifact and would stay weaker even if Markdown parsed
# trivially. Two more bypasses (a multi-backtick code span, an explicit link) were found in the round
# after five were closed, with more unhandled; the census and the argument are recorded at the deleted
# site in `scripts/flow/roborev-waiver-scan.py`. Retrievability, not prose, is what enforces
# NOT-DROPPED.
#
# BOTH DIRECTIONS, as #3564 required of its own fix: a one-direction family cannot distinguish a
# correct gate from one that fails everything, and `--recheck-job` is the only path either
# authorization can travel — so a gate that over-fails would break the break-glass instead of the
# deadlock.
FINDINGS_TEXT_3='## Findings\n- **Severity**: High\nProblem: the first one.\n- **Severity**: Medium\nProblem: the second one.\n- **Severity**: Low\nProblem: a third one arrived.\n## Summary\n3 findings.'
d_issues='3602,3613'
d_reason='both already filed and lead-deferred; 5937937 in / 5703168 cached'
d_grant="roborev-defer: findings issues=$d_issues count=2 base=$w_base head=$w_head job=4656 reason=$d_reason"
# The fixture state EVERY grant case needs: a findings-bearing record for job 4656, a prompt that
# matches this fixture's own census (so `prompt-content:` PASSes and the deferral is the SOLE thing
# under test), and both deferred issues retrievable.
df_grant_fixture() {
  STUB_ANNOUNCE_SHA="$w_head"
  STUB_PROMPT="$PROMPT_WITH_W_PATHS"
  STUB_VERDICT_FIELD='F'
  STUB_RECORD_OUTPUT="$FINDINGS_TEXT"
  STUB_GH_ISSUES='3602 3613'
}

printf '== (df1) #3626: a GRANTED, MATCHED deferral reports DEFERRED and reaches PASS ==\n'
# THE ACCEPTANCE CASE FOR THE WHOLE MECHANISM. Every other key is made to PASS (hence the
# census-matching prompt), so the deferral is the SOLE reason this run reaches a verdict at all —
# with any other key failing, the run would FAIL for a reason that was never in question and the case
# would keep passing if the fix were reverted.
reset_stub
df_grant_fixture
STUB_GH_COMMENTS="\001pmcfadin\n$d_grant\n"
run_wrapper "$w_work" --recheck-job 4656
assert_verdict 'case (df1)' PASS 0
assert_no_marker_form 'case (df1)'
assert_says 'case (df1) the findings token is DEFERRED, naming the count, the issues, the author and the job' \
  "^findings: DEFERRED \(2, issues=#3602,#3613, authorized @pmcfadin, job 4656\)\$"
assert_says 'case (df1) the deferral key records author, issues, count, the whole scope and the verbatim reason' \
  "^deferral: GRANTED \(author=@pmcfadin issues=$d_issues count=2 scope=base=$w_base head=$w_head job=4656 reason=$d_reason\)\$"
# A DEFERRED RUN IS NOT GREPPABLE AS CLEAN. `NONE` stays reachable only from the record's structured
# verdict letter, so no reader looking for a clean review can find a deferred one.
assert_lacks 'case (df1) a deferral NEVER yields findings: NONE' '^findings: NONE'
assert_lacks 'case (df1) and never spells itself as a findings PASS' '^findings: PASS'
# THE ISOLATION CLAIM, PINNED RATHER THAN ASSERTED IN PROSE: reaching PASS proves no key failed the
# grammar scan, not that the others affirmatively passed.
assert_says 'case (df1) the reviewer-diff delivery key affirmatively PASSed (the deferral is the SOLE cause)' \
  '^prompt-content: PASS \(2/2 code census paths present\)$'
assert_says 'case (df1) and completion was re-asserted from the record' '^review-completed: PASS$'
# A DEFERRED PASS CAN NEVER BE PASTED AS A FRESH CLEAN REVIEW: the mode is declared in the first key.
assert_says 'case (df1) the block still declares the recheck mode first' \
  '^MODE: recheck \(job 4656 re-decided from its job record; NO review was enqueued — not evidence of a fresh review\)$'
assert_says 'case (df1) the NOTICE records that this is NOT a clean review' 'This is NOT a clean review'
reset_stub

printf '== (df2) #3626: NO marker at all — deferral: NONE teaching both channel rules, FAIL ==\n'
reset_stub
df_grant_fixture
run_wrapper "$w_work" --recheck-job 4656
assert_verdict 'case (df2)' FAIL 1
assert_no_marker_form 'case (df2)'
assert_says 'case (df2) the findings stand as PRESENT' '^findings: PRESENT \(2\)$'
assert_says 'case (df2) the NONE cause teaches the sole-content and top-level rules' \
  '^deferral: NONE \(no findings-deferral comment for this review: the authorization must be the SOLE NONBLANK CONTENT of a TOP-LEVEL PR comment — one inside prose, a code fence, a quote or a review body is not read\)$'
assert_lacks 'case (df2) nothing is deferred' '^findings: DEFERRED'
# LAYER 3 (#3312 job 23): no emitted diagnostic carries ANY part of the marker, because a summary
# block pasted into a PR comment would otherwise authorize the next run.
assert_lacks 'case (df2) no part of the deferral marker appears anywhere in the output' 'roborev-defer'
assert_says 'case (df2) the diagnostic points at --help instead of printing a form' \
  'THE EXACT FORM IS DELIBERATELY NOT PRINTED HERE'
assert_says 'case (df2) and it names the scope an authorization would have to bind' \
  "head $w_head, job 4656, count 2"
reset_stub

printf '== (df3) #3626: a marker naming a DIFFERENT JOB is STALE (a re-run inherits nothing) ==\n'
reset_stub
df_grant_fixture
STUB_GH_COMMENTS="\001pmcfadin\nroborev-defer: findings issues=$d_issues count=2 base=$w_base head=$w_head job=9999 reason=judged an earlier review\n"
run_wrapper "$w_work" --recheck-job 4656
assert_verdict 'case (df3)' FAIL 1
assert_no_marker_form 'case (df3)'
assert_says 'case (df3) the marker names another review' \
  "^deferral: STALE \(the marker names a different review — job \(9999 != 4656\)"
assert_says 'case (df3) and says an authorization may not outlive the review it judged' \
  'a deferral may not outlive the review its authorizer judged'
assert_lacks 'case (df3) nothing is deferred' '^findings: DEFERRED'
reset_stub

printf '== (df3b) #3626: a marker naming a DIFFERENT HEAD is STALE (a push needs a fresh one) ==\n'
reset_stub
df_grant_fixture
STUB_GH_COMMENTS="\001pmcfadin\nroborev-defer: findings issues=$d_issues count=2 base=$w_base head=0000000000000000000000000000000000000000 job=4656 reason=written before the push\n"
run_wrapper "$w_work" --recheck-job 4656
assert_verdict 'case (df3b)' FAIL 1
assert_no_marker_form 'case (df3b)'
assert_says 'case (df3b) the head divergence is named' \
  "^deferral: STALE \(the marker names a different review — head \(0000000000000000000000000000000000000000 != $w_head\)"
reset_stub

printf '== (df4) #3626: a marker-only comment MISSING a field is MALFORMED ==\n'
reset_stub
df_grant_fixture
STUB_GH_COMMENTS="\001pmcfadin\nroborev-defer: findings issues=$d_issues base=$w_base head=$w_head job=4656 reason=no count field\n"
run_wrapper "$w_work" --recheck-job 4656
assert_verdict 'case (df4)' FAIL 1
assert_no_marker_form 'case (df4)'
assert_says 'case (df4) a missing field does not match the required form' \
  '^deferral: MALFORMED \(the line begins an authorization of this kind but does not match its required form'
assert_lacks 'case (df4) and never grants' '^findings: DEFERRED'
reset_stub

printf '== (df4b) #3626: the field ORDER and field-value BOUNDARIES are enforced by one pattern ==\n'
reset_stub
df_grant_fixture
STUB_GH_COMMENTS="\001pmcfadin\nroborev-defer: findings count=2 issues=$d_issues base=$w_base head=$w_head job=4656x reason=order and boundary\n"
run_wrapper "$w_work" --recheck-job 4656
assert_verdict 'case (df4b)' FAIL 1
assert_no_marker_form 'case (df4b)'
assert_says 'case (df4b) a re-ordered, unbounded marker is MALFORMED' \
  '^deferral: MALFORMED \(the line begins an authorization of this kind but does not match its required form'
reset_stub

printf '== (df4c) #3626: a PLACEHOLDER reason is refused, trimmed BEFORE it is judged ==\n'
# Both classic defeats in one case: an unsubstituted <...> (a pasted template) and `TODO` with
# trailing whitespace (compared before trimming, `TODO ` is not equal to `todo`).
reset_stub
df_grant_fixture
STUB_GH_COMMENTS="\001pmcfadin\nroborev-defer: findings issues=$d_issues count=2 base=$w_base head=$w_head job=4656 reason=<why>\n"
run_wrapper "$w_work" --recheck-job 4656
assert_verdict 'case (df4c)' FAIL 1
assert_no_marker_form 'case (df4c)'
assert_says 'case (df4c) a pasted template is MALFORMED' \
  '^deferral: MALFORMED \(the marker is missing a-substituted-reason'
reset_stub
df_grant_fixture
STUB_GH_COMMENTS="\001pmcfadin\nroborev-defer: findings issues=$d_issues count=2 base=$w_base head=$w_head job=4656 reason=TODO   \n"
run_wrapper "$w_work" --recheck-job 4656
assert_verdict 'case (df4c trailing whitespace)' FAIL 1
assert_no_marker_form 'case (df4c trailing whitespace)'
assert_says 'case (df4c) the trimmed value is judged' \
  "^deferral: MALFORMED \(the marker is missing a-substantive-reason \(the reason 'TODO' is a bare placeholder\)"
reset_stub

printf '== (df4d) #3626: a marker-only comment that is the BARE STEM is MALFORMED, not silent ==\n'
# ROBOREV ROUND 2 (Low): the attempt test was the stem plus a MANDATORY TRAILING SPACE, so a comment
# reading EXACTLY `roborev-defer: findings` — an authorization someone plainly meant to write and then
# truncated — was not recognised as an attempt at all and reported `NONE` ("no authorization exists").
# That is a FAIL-QUIET on an attempted authorization: the author re-reads the syntax they typed, sees
# the prefix, and concludes the mechanism is broken. An attempt is now the stem plus whitespace OR
# END OF LINE, and the one anchored pattern decides malformed-ness. Both spellings are fixtured,
# because they are two different code paths through the line-splitting (`rest == ""` vs a trailing
# newline making the marker the sole NONBLANK line).
reset_stub
df_grant_fixture
STUB_GH_COMMENTS="\001pmcfadin\nroborev-defer: findings"
run_wrapper "$w_work" --recheck-job 4656
assert_verdict 'case (df4d bare stem)' FAIL 1
assert_no_marker_form 'case (df4d bare stem)'
assert_says 'case (df4d) the bare stem is a MALFORMED attempt, not an absent authorization' \
  '^deferral: MALFORMED \(the line begins an authorization of this kind but does not match its required form'
assert_lacks 'case (df4d) and it is NOT reported as if no marker existed' '^deferral: NONE'
reset_stub
df_grant_fixture
STUB_GH_COMMENTS="\001pmcfadin\nroborev-defer: findings\n"
run_wrapper "$w_work" --recheck-job 4656
assert_verdict 'case (df4d stem plus newline)' FAIL 1
assert_no_marker_form 'case (df4d stem plus newline)'
assert_says 'case (df4d stem plus newline) is MALFORMED too' \
  '^deferral: MALFORMED \(the line begins an authorization of this kind but does not match its required form'
assert_lacks 'case (df4d stem plus newline) not silent' '^deferral: NONE'
reset_stub
# A DIFFERENT WORD IS STILL NOT AN ATTEMPT: the boundary is TESTED rather than dropped, so
# `roborev-defer: findingsfoo` stays silent — nobody attempted this authorization.
df_grant_fixture
STUB_GH_COMMENTS="\001pmcfadin\nroborev-defer: findingsfoo issues=3602 count=2 base=$w_base head=$w_head job=4656 reason=a different word\n"
run_wrapper "$w_work" --recheck-job 4656
assert_verdict 'case (df4d different word)' FAIL 1
assert_says 'case (df4d different word) is not an attempt at this marker' \
  '^deferral: NONE \(no findings-deferral comment'
reset_stub

printf '== (df5) #3626: the same marker from a NON-ALLOWLISTED author is UNAUTHORIZED ==\n'
# This is a PUBLIC repository and a failing block PRINTS base/head/job and the observed count, so
# without the allowlist any commenter could copy them and clear a merge gate's findings.
reset_stub
df_grant_fixture
STUB_GH_COMMENTS="\001drive-by-contributor\nroborev-defer: findings issues=$d_issues count=2 base=$w_base head=$w_head job=4656 reason=copied from the public failing block\n"
run_wrapper "$w_work" --recheck-job 4656
assert_verdict 'case (df5)' FAIL 1
assert_no_marker_form 'case (df5)'
assert_says 'case (df5) the marker was fine and the author was not' \
  "^deferral: UNAUTHORIZED \(the marker is well-formed and names this review, but its author '@drive-by-contributor' is not on the deferral allowlist"
assert_lacks 'case (df5) a stranger cannot defer' '^findings: DEFERRED'
reset_stub

printf '== (df5b) #3626: an UNAUTHORIZED marker does not shadow an allowlisted one ==\n'
# The refusal must not be sticky: the LAST GRANTED marker wins, so a stranger commenting first cannot
# deny a legitimate authorization posted afterwards.
reset_stub
df_grant_fixture
STUB_GH_COMMENTS="\001drive-by-contributor\nroborev-defer: findings issues=$d_issues count=2 base=$w_base head=$w_head job=4656 reason=stranger first\n\001pmcfadin\n$d_grant\n"
run_wrapper "$w_work" --recheck-job 4656
assert_verdict 'case (df5b)' PASS 0
assert_no_marker_form 'case (df5b)'
assert_says 'case (df5b) the allowlisted authorization still grants' '^deferral: GRANTED \(author=@pmcfadin'
reset_stub

printf '== (df6) #3626: the OBSERVED count must EQUAL the authorized one — COUNT-MISMATCH ==\n'
# THE AFFIRMATIVE HALF OF THE BINDING (#3586: never derive a pass from the absence of a bad signal).
# The authorization covers TWO findings and the job reports THREE, which is exactly what a NEW finding
# arriving at the same head looks like — so the undeferred set is non-empty and the FAIL stands.
reset_stub
df_grant_fixture
STUB_RECORD_OUTPUT="$FINDINGS_TEXT_3"
STUB_GH_COMMENTS="\001pmcfadin\n$d_grant\n"
run_wrapper "$w_work" --recheck-job 4656
assert_verdict 'case (df6)' FAIL 1
assert_no_marker_form 'case (df6)'
assert_says 'case (df6) the observed findings count is reported unchanged' '^findings: PRESENT \(3\)$'
assert_says 'case (df6) the mismatch is its own cause, naming both numbers' \
  '^deferral: COUNT-MISMATCH \(the marker authorizes 2 finding\(s\) but this job reports 3'
assert_says 'case (df6) and it says a new finding must not ride an older authorization' \
  'A new finding at the same head raises the observed count and must not ride an older authorization'
assert_lacks 'case (df6) nothing is deferred' '^findings: DEFERRED'
reset_stub

printf '== (df6b) #3626: a PRE-AUTHORIZATION (count declared before the findings were read) fails ==\n'
# The residual #3312 names — an authorized human can authorize carelessly — is at least made
# DETECTABLE for the count: a marker written before the job finished cannot know the number.
reset_stub
df_grant_fixture
STUB_GH_COMMENTS="\001pmcfadin\nroborev-defer: findings issues=$d_issues count=1 base=$w_base head=$w_head job=4656 reason=pre-authorized before the review finished\n"
run_wrapper "$w_work" --recheck-job 4656
assert_verdict 'case (df6b)' FAIL 1
assert_no_marker_form 'case (df6b)'
assert_says 'case (df6b) a count that was guessed does not match' \
  '^deferral: COUNT-MISMATCH \(the marker authorizes 1 finding\(s\) but this job reports 2'
reset_stub

printf '== (df7) #3626: an issue GitHub says DOES NOT EXIST is ISSUE-ABSENT, not skipped ==\n'
# A deferral must name a FILED issue: one GitHub answers does not exist is a deferral into a void,
# i.e. a DROPPED finding wearing a link. Fail-closed under its own cause.
#
# RETRIEVABILITY IS NOW THE LOAD-BEARING LEG of the disposition — the PR-body link check it used to
# share that job with was deleted (see this family's header) — so it is pinned THREE-VALUED here:
# (df7) the issue is verifiably absent, (df7b) its existence could not be ASKED, and each has its own
# cause because they are different operator actions.
reset_stub
df_grant_fixture
STUB_GH_ISSUES='3602'
STUB_GH_COMMENTS="\001pmcfadin\n$d_grant\n"
run_wrapper "$w_work" --recheck-job 4656
assert_verdict 'case (df7)' FAIL 1
assert_no_marker_form 'case (df7)'
assert_says 'case (df7) the absent issue is named, as an ANSWER from GitHub' \
  '^deferral: ISSUE-ABSENT \(GitHub answered that issue #3613 DOES NOT EXIST in this repository'
assert_says 'case (df7) and it says a deferral must name a filed issue' 'A deferral must name a FILED issue'
assert_says 'case (df7) the remedy is the marker or the missing issue, not the network' \
  'Check the number in the marker, file the issue, then re-authorize'
assert_lacks 'case (df7) nothing is deferred' '^findings: DEFERRED'
reset_stub

printf '== (df7b) #3626: a COULD-NOT-ASK is ISSUE-UNVERIFIABLE — never read as verified ==\n'
# THE FAIL-OPEN THIS LEG MUST NOT HAVE (lead condition 1). `gh issue view` EXITS 1 FOR BOTH a missing
# issue and an auth failure — measured on gh 2.98.0, `HTTP 401: Bad credentials` vs `GraphQL: Could
# not resolve to an issue or pull request with the number of N.` — so an exit-code-only test is the
# two-valued predicate that always picks the permissive answer, and it would grant a deferral over
# issues NOBODY CONFIRMED EXIST. Everything else about this fixture is a PERFECT authorization: the
# same marker that grants in (df1), the right author, the right scope, the matching count. The ONLY
# difference is that the box cannot ask GitHub — and that alone must block the grant.
reset_stub
df_grant_fixture
STUB_GH_ISSUE_ERR='HTTP 401: Bad credentials (https://api.github.com/graphql)'
STUB_GH_COMMENTS="\001pmcfadin\n$d_grant\n"
run_wrapper "$w_work" --recheck-job 4656
assert_verdict 'case (df7b)' FAIL 1
assert_no_marker_form 'case (df7b)'
assert_says 'case (df7b) the could-not-ask has its OWN state, textually distinct from ISSUE-ABSENT' \
  "^deferral: ISSUE-UNVERIFIABLE \('gh issue view 3602' failed WITHOUT answering that the issue does not exist"
assert_says 'case (df7b) it carries the diagnostic it could not interpret' 'HTTP 401: Bad credentials'
assert_says 'case (df7b) it says this is not an answer' 'this is a could-not-ask, not an answer'
assert_says 'case (df7b) and it sends the operator at the network, NOT at the marker' \
  'fix the ability to reach GitHub \(auth, network, rate limit\) and re-run; do NOT change the marker'
# THE TWO NON-GRANTING STATES MUST NOT BE CONFUSABLE: a run that could not ask must never print the
# state that means GitHub gave an answer.
assert_lacks 'case (df7b) a could-not-ask never reports itself as an absent issue' '^deferral: ISSUE-ABSENT'
assert_lacks 'case (df7b) and nothing is deferred' '^findings: DEFERRED'
reset_stub

printf '== (df7c) #3626: the NAIVE two-valued form WOULD have granted the (df7b) fixture (mutant) ==\n'
# THE RED/GREEN CONTRAST FOR (df7b), because an assert that a run FAILs is satisfied by a run that
# fails for any reason at all. The mutant is the realistic naive implementation — "verified unless
# GitHub explicitly said not-found", i.e. a could-not-ask collapsed onto PRESENT — applied to a COPY.
# If it grants and reaches PASS on the very fixture (df7b) refuses, then (df7b) is measuring the
# three-valued logic and nothing else.
#
# THE CONTROL RUNS FIRST AND IS NOT OPTIONAL: the UNPATCHED copy must FAIL this fixture exactly as the
# real wrapper does, so a mutant PASS cannot be an artefact of how the copy was made.
reset_stub
_dfr_dir="$tmp/deferral-retrievability"
mkdir -p "$_dfr_dir"
cp "$WRAPPER_REAL" "$SCRIPT_DIR/../flow/roborev-review-oracles.sh" \
  "$SCRIPT_DIR/../flow/roborev-review-checks.sh" "$SCAN_TOOL" "$_dfr_dir/"
if [ -f "$SCRIPT_DIR/../flow/roborev-job-facts.py" ]; then
  cp "$SCRIPT_DIR/../flow/roborev-job-facts.py" "$_dfr_dir/"
fi
df_grant_fixture
STUB_GH_ISSUE_ERR='HTTP 401: Bad credentials (https://api.github.com/graphql)'
STUB_GH_COMMENTS="\001pmcfadin\n$d_grant\n"
run_wrapper --wrapper "$_dfr_dir/roborev-review.sh" "$w_work" --recheck-job 4656
assert_verdict 'case (df7c control) the UNPATCHED copy refuses the could-not-ask' FAIL 1
assert_says 'case (df7c control) under the three-valued state' '^deferral: ISSUE-UNVERIFIABLE'
if sed_inplace_verified "$_dfr_dir/roborev-review-oracles.sh" \
  's/^roborev_issue_retrievability() {$/roborev_issue_retrievability() {\
  ROBOREV_ISSUE_STATE="present"; ROBOREV_ISSUE_DETAIL="naive two-valued form"\
  _naive=$(cd "$REPO" \&\& gh issue view "$1" --json number 2>\&1) || true\
  case "$_naive" in *"not resolve to an issue"*) ROBOREV_ISSUE_STATE="absent" ;; esac\
  return 0/' \
  'naive two-valued form' ''; then
  ok 'case (df7c): the naive-retrievability patch was really applied to the copy'
  run_wrapper --wrapper "$_dfr_dir/roborev-review.sh" "$w_work" --recheck-job 4656
  assert_verdict 'case (df7c) the naive form GRANTS over issues nobody confirmed exist' PASS 0
  assert_says 'case (df7c) the mutant reaches a GRANTED deferral on the (df7b) fixture' \
    '^deferral: GRANTED \(author=@pmcfadin'
  assert_says 'case (df7c) and defers the findings' '^findings: DEFERRED \(2, issues=#3602,#3613'
else
  bad 'case (df7c): could not patch the copied oracles file, so the naive form was never exercised — without this contrast (df7b) proves only that SOMETHING failed'
fi
reset_stub

printf '== (df7d) #3626 round 3: a CLOSED issue is ISSUE-CLOSED — retrievable is NOT tracked ==\n'
# `gh issue view` RETURNS THE NUMBER AND EXITS 0 FOR A CLOSED ISSUE, so a number-only test made "the
# finding is tracked" satisfiable by an issue closed as a duplicate three weeks ago: `present` =>
# `GRANTED` => `RESULT: PASS`, the finding permanently untracked, and the block asserting it was filed.
# That is the exact thing this leg exists to prevent, so OPEN is required — DELIBERATELY STRONGER than
# the lead's literal "retrievable" condition, because the claim made at the call site, in the scanner
# and in the spec is the stronger NOT-DROPPED one.
#
# Everything else in this fixture is a PERFECT authorization — the marker that grants in (df1), the
# right author, the right scope, the matching count — so the CLOSED state is the sole difference.
reset_stub
df_grant_fixture
STUB_GH_ISSUES='3602'
STUB_GH_ISSUES_CLOSED='3613'
STUB_GH_COMMENTS="\001pmcfadin\n$d_grant\n"
run_wrapper "$w_work" --recheck-job 4656
assert_verdict 'case (df7d)' FAIL 1
assert_no_marker_form 'case (df7d)'
assert_says 'case (df7d) the closed issue has its OWN state and cause' \
  '^deferral: ISSUE-CLOSED \(GitHub answered that issue #3613 is CLOSED'
assert_says 'case (df7d) it says a closed issue means the finding is DROPPED with a link attached' \
  'which is the finding being DROPPED with a link attached'
assert_says 'case (df7d) the strictness is declared, with a recoverable remedy' \
  "stricter than 'retrievable' on purpose: reopen #3613, or file a fresh tracking issue"
# THE FOUR STATES MUST NOT BE CONFUSABLE: a closed issue is not an absent one and not a could-not-ask.
assert_lacks 'case (df7d) a closed issue never reports itself as absent' '^deferral: ISSUE-ABSENT'
assert_lacks 'case (df7d) nor as a could-not-ask' '^deferral: ISSUE-UNVERIFIABLE'
assert_lacks 'case (df7d) and nothing is deferred' '^findings: DEFERRED'
reset_stub

printf '== (df7e) #3626 round 3: the NUMBER-ONLY form WOULD have granted the (df7d) fixture (mutant) ==\n'
# THE RED/GREEN CONTRAST FOR (df7d): a run that FAILs satisfies (df7d) for any reason at all, so the
# naive implementation — `--json number --jq .number`, exactly what this leg asked for until now — is
# applied to a COPY and must GRANT on the same fixture. CONTROL FIRST AND NOT OPTIONAL: the UNPATCHED
# copy must refuse it, so a mutant PASS cannot be an artefact of how the copy was made.
reset_stub
_dfc_dir="$tmp/deferral-closed"
mkdir -p "$_dfc_dir"
cp "$WRAPPER_REAL" "$SCRIPT_DIR/../flow/roborev-review-oracles.sh" \
  "$SCRIPT_DIR/../flow/roborev-review-checks.sh" "$SCAN_TOOL" "$_dfc_dir/"
if [ -f "$SCRIPT_DIR/../flow/roborev-job-facts.py" ]; then
  cp "$SCRIPT_DIR/../flow/roborev-job-facts.py" "$_dfc_dir/"
fi
df_grant_fixture
STUB_GH_ISSUES='3602'
STUB_GH_ISSUES_CLOSED='3613'
STUB_GH_COMMENTS="\001pmcfadin\n$d_grant\n"
run_wrapper --wrapper "$_dfc_dir/roborev-review.sh" "$w_work" --recheck-job 4656
assert_verdict 'case (df7e control) the UNPATCHED copy refuses the closed issue' FAIL 1
assert_says 'case (df7e control) under the closed state' '^deferral: ISSUE-CLOSED'
if sed_inplace_verified "$_dfc_dir/roborev-review-oracles.sh" \
  's/^roborev_issue_retrievability() {$/roborev_issue_retrievability() {\
  ROBOREV_ISSUE_STATE="unverifiable"; ROBOREV_ISSUE_DETAIL="number-only form"\
  _naive=$(cd "$REPO" \&\& gh issue view "$1" --json number --jq .number 2>\/dev\/null) || true\
  [ "$_naive" = "$1" ] \&\& ROBOREV_ISSUE_STATE="present"\
  return 0/' \
  'number-only form' ''; then
  ok 'case (df7e): the number-only patch was really applied to the copy'
  run_wrapper --wrapper "$_dfc_dir/roborev-review.sh" "$w_work" --recheck-job 4656
  assert_verdict 'case (df7e) the number-only form GRANTS over an issue closed as a duplicate' PASS 0
  assert_says 'case (df7e) the mutant reaches a GRANTED deferral on the (df7d) fixture' \
    '^deferral: GRANTED \(author=@pmcfadin'
  assert_says 'case (df7e) and defers the findings to a closed issue' '^findings: DEFERRED \(2, issues=#3602,#3613'
else
  bad 'case (df7e): could not patch the copied oracles file, so the number-only form was never exercised — without this contrast (df7d) proves only that SOMETHING failed'
fi
reset_stub

printf '== (df7f) #3626 round 3: the disposition backstop COUNTS VERIFICATIONS, it does not test the string ==\n'
# THE ONE PROPERTY WITH NO ASSERT IN EITHER SUITE UNTIL NOW (roborev job 229 blocker 2). The backstop
# used to be `[ -z "$ROBOREV_DEFERRAL_ISSUES" ]`, i.e. a NON-EMPTINESS test standing in for a
# VERIFICATION test. Reproduced: `ROBOREV_DEFERRAL_ISSUES=","` passes `[ -z ]`, `${//,/ }` yields
# `" "`, the UNQUOTED expansion splits into ZERO WORDS, the loop body never runs, and the function
# returns with the state still `granted` — a grant with not one `gh issue view` executed.
#
# PROBED DIRECTLY, because the marker pattern's `issues=` cannot express that value today: the whole
# point of a backstop is not to depend on an upstream check still being there, so the case must reach
# the function the way a future loosening of `issues=` would. The mutant is the naive `-z` form.
_df7f_probe="$tmp/df7f-probe.sh"
_df7f_out="$tmp/df7f-out.txt"
cat >"$_df7f_probe" <<'DF7F'
set -uo pipefail
REPO="$2"
. "$1"   # oracles
# The scanner is replaced wholesale rather than redirected: a granted state carrying a comma-only
# issue list is not expressible through the real marker pattern, and the point is what the BACKSTOP
# does with one, not what the pattern admits.
roborev_findings_deferral_lookup() { :; }
probe_disposition() { # probe_disposition <issues-value>
  ROBOREV_DEFERRAL_STATE=granted
  ROBOREV_DEFERRAL_ISSUES="$1"
  ROBOREV_DEFERRAL_DETAIL=""
  _probe_disposition_body
  printf 'issues=[%s] state=%s detail=%s\n' "$1" "$ROBOREV_DEFERRAL_STATE" "$ROBOREV_DEFERRAL_DETAIL"
}
DF7F
# THE SUBJECT IS THE REAL FUNCTION'S OWN DISPOSITION CODE, lifted out of the shipped file rather than
# restated here: a re-typed copy would drift and the probe would measure the copy. The extraction runs
# from the backstop's own banner to the function's closing brace.
awk '/^  # ===== THE BACKSTOP COUNTS VERIFICATIONS PERFORMED/ { inb = 1 }
     inb && /^}$/ { print "}"; exit }
     inb { print }' "$ORACLES_SRC" >"$tmp/df7f-body.txt"
{
  printf '_probe_disposition_body() {\n'
  sed 's/^  //' "$tmp/df7f-body.txt" | sed '$d'
  printf '  return 0\n}\n'
  printf 'probe_disposition ","\nprobe_disposition "3602,,3613"\nprobe_disposition "3602"\n'
} >>"$_df7f_probe"
# EVERY NUMBER THE PROBE NAMES MUST BE RETRIEVABLE, so the COUNT is the only thing left that can
# refuse. Found here: with 3613 unknown to the stub, the `3602,,3613` case was refused as ISSUE-ABSENT
# — non-granting, but by the WRONG LEG, so the case would have proved nothing about the count.
# THE STUB MUST BE ON PATH IN THE PROBE, NOT JUST IN `run_wrapper` (found here): without it the probe
# reached the REAL `gh`, which answered "none of the git remotes ... point to a known GitHub host" —
# an unrecognised diagnostic, so `issue-unverifiable`. The case would then have reported the affirmative
# count as over-strict when the count was never what refused it.
STUB_GH_ISSUES='3602 3613'
if [ -s "$tmp/df7f-body.txt" ] \
  && PATH="$stubbin:$PATH" bash "$_df7f_probe" "$ORACLES_SRC" "$w_work" >"$_df7f_out" 2>&1; then
  if grep -q '^issues=\[,\] state=unavailable' "$_df7f_out"; then
    ok 'case (df7f): a comma-only issue list traverses ZERO issues, so the affirmative count refuses it (state unavailable, not granted)'
  else
    bad "case (df7f): the comma-only list did not fail closed: $(cat "$_df7f_out")"
  fi
  # AN EMPTY FIELD IS THE SAME DEFECT ONE SHAPE OVER: `3602,,3613` declares 3 fields and the split
  # traverses 2, so one declared field is never checked. A non-emptiness test cannot see it either.
  if grep -q '^issues=\[3602,,3613\] state=unavailable' "$_df7f_out"; then
    ok 'case (df7f): an EMPTY issue field is refused too — 3 declared, 2 traversed, so one field was never checked'
  else
    bad "case (df7f): an empty issue field was not refused: $(grep '^issues=\[3602,,3613\]' "$_df7f_out" || cat "$_df7f_out")"
  fi
  if grep -q '^issues=\[3602\] state=granted' "$_df7f_out"; then
    ok 'case (df7f): and a real single-issue list still grants, so the backstop is not merely refusing everything'
  else
    bad "case (df7f): the affirmative form refused a legitimate one-issue list, which would break the break-glass: $(cat "$_df7f_out")"
  fi
else
  bad "case (df7f): the disposition probe did not run (body $(wc -c <"$tmp/df7f-body.txt" 2>/dev/null) bytes): $(cat "$_df7f_out" 2>/dev/null)"
fi
# THE MUTANT: the naive `-z`-only backstop, which GRANTS on the comma-only list. Without this
# contrast the case above proves only that SOMETHING returned `unavailable`.
_df7f_naive="$tmp/df7f-naive.sh"
sed 's/^if \[ "\$verified_issues" -ne "\$declared_issues" \]; then$/if false; then/' \
  "$_df7f_probe" >"$_df7f_naive"
if grep -qF 'if false; then' "$_df7f_naive"; then
  if PATH="$stubbin:$PATH" bash "$_df7f_naive" "$ORACLES_SRC" "$w_work" >"$_df7f_out" 2>&1 \
    && grep -q '^issues=\[,\] state=granted' "$_df7f_out"; then
    ok 'case (df7f mutant): with the affirmative count removed, the -z-only backstop GRANTS on a comma-only list — so the real refusal is measuring the count and nothing else'
  else
    bad "case (df7f mutant): the naive form did not grant, so the contrast establishes nothing: $(cat "$_df7f_out")"
  fi
else
  bad 'case (df7f mutant): the count-equality line could not be neutralised in the probe copy, so the naive form was never exercised'
fi
reset_stub

printf '== (df7g) #3626 round 3: an ABBREVIATED sha is MALFORMED, not STALE — both marker kinds ==\n'
# The documented form is 40 hex. The patterns admitted `{7,40}`, so an abbreviated sha MATCHED and then
# diverged from this run's 40-hex base/head — reported `STALE` ("the marker names a different review")
# when the truth is that it names THIS review in a spelling the form does not permit. An authorizer sent
# to re-check WHICH REVIEW they named finds nothing wrong with it. BOTH KINDS, because they share one
# parser and a field rule that holds for one marker and not the other is a divergence in a channel rule.
reset_stub
df_grant_fixture
STUB_GH_COMMENTS="\001pmcfadin\nroborev-defer: findings issues=$d_issues count=2 base=${w_base:0:12} head=${w_head:0:12} job=4656 reason=$d_reason\n"
run_wrapper "$w_work" --recheck-job 4656
assert_verdict 'case (df7g) deferral' FAIL 1
assert_no_marker_form 'case (df7g) deferral'
assert_says 'case (df7g) an abbreviated sha is a FORM defect' '^deferral: MALFORMED \(the line begins an authorization of this kind but does not match its required form'
assert_lacks 'case (df7g) and is NOT reported as naming another review' '^deferral: STALE'
assert_lacks 'case (df7g) nothing is deferred' '^findings: DEFERRED'
reset_stub
# THE WAIVER KIND, over its own key. The prompt deliberately does NOT match the census, so
# `prompt-content:` reaches its ABSENCE branch and looks for a waiver.
STUB_ANNOUNCE_SHA="$w_head"
STUB_PROMPT='no census path appears here at all'
STUB_GH_COMMENTS="\001pmcfadin\nroborev-waive: prompt-content-absent base=${w_base:0:7} head=${w_head:0:7} job=4656 reason=abbreviated on purpose\n"
run_wrapper "$w_work" --recheck-job 4656
assert_verdict 'case (df7g) waiver' FAIL 1
assert_no_marker_form 'case (df7g) waiver'
assert_says 'case (df7g) the waiver kind reports the same FORM defect' '^waiver: MALFORMED \(the line begins an authorization of this kind but does not match its required form'
assert_lacks 'case (df7g) and not STALE' '^waiver: STALE'
reset_stub

printf '== (df7h) #3626 round 3: the reason is recorded VERBATIM — repeated spaces and a tab survive ==\n'
# `collapse()` rewrote INTERNAL whitespace, so a granted reason reached `deferral: GRANTED (...)`
# altered while the spec, `--help` and the emitted NOTICE all promise it is recorded VERBATIM. An
# authorization whose recorded terms are not the terms that were given is a weaker audit trail than it
# claims to be, and the claim is the whole value of the record. What actually has to hold is only that
# a value occupies ONE LINE.
#
# TWO BOUNDARIES, AND ONLY ONE OF THEM MAY REWRITE ANYTHING. The scanner must not touch internal
# whitespace at all; the BLOCK boundary (`roborev_safe_line`) then renders a control character as a
# VISIBLE escape, which is a different and legitimate thing — it keeps a control byte out of a line a
# reader greps, without silently deleting information. So a repeated SPACE must survive byte-for-byte,
# and a TAB must appear as the two-character `\t` rather than being COLLAPSED into a single space.
reset_stub
df_grant_fixture
_df7h_reason='two  spaces and'$'\t''a tab; 5937937 in'
_df7h_block='two  spaces and\ta tab; 5937937 in'
STUB_GH_COMMENTS="\001pmcfadin\nroborev-defer: findings issues=$d_issues count=2 base=$w_base head=$w_head job=4656 reason=$_df7h_reason\n"
run_wrapper "$w_work" --recheck-job 4656
assert_verdict 'case (df7h)' PASS 0
assert_no_marker_form 'case (df7h)'
if grep -qF "reason=$_df7h_block)" "$OUT"; then
  ok 'case (df7h): the granted reason survives verbatim — the repeated space is intact and the tab is a VISIBLE \t, not a collapsed space'
else
  bad "case (df7h): the granted reason was rewritten on the way to the block: $(grep '^deferral: ' "$OUT" || printf '<no deferral key>')"
fi
assert_lacks 'case (df7h) and the reason is NOT whitespace-collapsed' 'reason=two spaces and a tab'
assert_one_result_line 'case (df7h)'
reset_stub

printf '== (df7i) #3626 round 3: a reason naming a marker STEM is refused, both kinds ==\n'
# THE INVARIANT IS OVER THE OUTPUT, AND THE STRUCTURAL ASSERT ONLY COVERS THE CODE. A granted reason is
# interpolated into `deferral:`/`waiver: GRANTED (... reason=...)`, which reaches the summary block —
# and no emitted diagnostic may carry any part of a marker form, because blocks get pasted into PR
# comments as a matter of course (#3312 job 23). A RUNTIME reason can inject what no source scan sees.
# Refused rather than escaped: an authorizer has no legitimate need for a marker stem in a reason.
reset_stub
df_grant_fixture
STUB_GH_COMMENTS="\001pmcfadin\nroborev-defer: findings issues=$d_issues count=2 base=$w_base head=$w_head job=4656 reason=see the roborev-defer: findings line above\n"
run_wrapper "$w_work" --recheck-job 4656
assert_verdict 'case (df7i) deferral' FAIL 1
assert_says 'case (df7i) a stem-bearing reason is a FORM defect naming what to do' \
  '^deferral: MALFORMED \(the marker is missing a-stem-free-reason'
assert_no_marker_form 'case (df7i) deferral'
assert_lacks 'case (df7i) nothing is deferred' '^findings: DEFERRED'
reset_stub
STUB_ANNOUNCE_SHA="$w_head"
STUB_PROMPT='no census path appears here at all'
STUB_GH_COMMENTS="\001pmcfadin\nroborev-waive: prompt-content-absent base=$w_base head=$w_head job=4656 reason=quoting roborev-waive in the reason\n"
run_wrapper "$w_work" --recheck-job 4656
assert_verdict 'case (df7i) waiver' FAIL 1
assert_says 'case (df7i) the waiver kind inherits the refusal BY CALL' \
  '^waiver: MALFORMED \(the marker is missing a-stem-free-reason'
assert_no_marker_form 'case (df7i) waiver'
reset_stub

printf '== (df7j) #3626 round 4: an AUTHOR whose LOGIN carries a marker keyword, both kinds ==\n'
# ROBOREV ROUND 4 (Low): THE SAME GUARANTEE, ONE FIELD OVER. Round 3 refused a stem-bearing REASON
# (df7i) and left the AUTHOR unguarded — and `unauthorized_detail()` interpolated the untrusted GitHub
# login VERBATIM into `waiver:`/`deferral: UNAUTHORIZED (... its author '@<login>' ...)`, which reaches
# the summary block. So the invariant held for the field the tests happened to cover and not for the
# field beside it: A PROPERTY ASSERTED ONLY WHERE IT CANNOT FAIL IS NOT ASSERTED, the same shape as the
# `assert_lacks` that was attached to the `NONE` state alone.
#
# WHAT THIS IS AND IS NOT, so a later reader neither treats it as a closed bypass nor reopens it as an
# open one: a GitHub login admits letters, digits and hyphens and NOT colons or spaces, so a login can
# contain `roborev-defer` but can NEVER contain a full stem (`roborev-defer: findings`); and the emitted
# line begins `deferral: UNAUTHORIZED (`, so `sole_marker_line`'s `startswith` test refuses it. This is
# spec conformance and invariant coverage, NOT a security layer — a two-token denylist at each process's
# ONE emit boundary (`safe_value` in the scanner, `roborev_safe_line` in the wrapper), so every value
# and every future key inherits it rather than needing its own fix.
reset_stub
df_grant_fixture
STUB_GH_COMMENTS="\001roborev-defer-fan\nroborev-defer: findings issues=$d_issues count=2 base=$w_base head=$w_head job=4656 reason=copied from the public failing block\n"
run_wrapper "$w_work" --recheck-job 4656
assert_verdict 'case (df7j) deferral' FAIL 1
assert_says 'case (df7j) the keyword is redacted out of the login, the rest of it intact' \
  "^deferral: UNAUTHORIZED \(the marker is well-formed and names this review, but its author '@\[authorization-keyword-redacted\]-fan' is not on the deferral allowlist"
assert_no_marker_form 'case (df7j) deferral'
assert_lacks 'case (df7j) a stranger cannot defer, whatever they are called' '^findings: DEFERRED'
reset_stub
# THE WAIVER KIND INHERITS THE SAME BOUNDARY — asserted, not assumed, because the two kinds share a
# renderer only as long as nobody gives one its own.
STUB_ANNOUNCE_SHA="$w_head"
STUB_PROMPT='no census path appears here at all'
STUB_GH_COMMENTS="\001Roborev-Waive-Guy\nroborev-waive: prompt-content-absent base=$w_base head=$w_head job=4656 reason=copied from the public failing block\n"
run_wrapper "$w_work" --recheck-job 4656
assert_verdict 'case (df7j) waiver' FAIL 1
assert_says 'case (df7j) the redaction is case-insensitive, as the marker keyword check is' \
  "^waiver: UNAUTHORIZED \(the marker is well-formed and names this review, but its author '@\[authorization-keyword-redacted\]-Guy' is not on the waiver allowlist"
assert_no_marker_form 'case (df7j) waiver'
assert_lacks 'case (df7j) and nothing is waived' '^prompt-content: WAIVED'
reset_stub
# THE SHELL HALF OF THE CLASS, WHICH THE TWO CASES ABOVE DO NOT REACH: an author is redacted inside the
# SCANNER, so the wrapper only ever renders text python already cleaned. `gh issue view`'s stderr is
# different — it enters `ROBOREV_ISSUE_DETAIL` in the shell, reaches `deferral:` without passing through
# the scanner at all, and is exactly the kind of value that grows an interpolation later. So the wrapper
# boundary (`roborev_safe_line`, already the one gate for every block value and every DETAILS line) is
# asserted on its own fixture rather than assumed from the python one.
reset_stub
df_grant_fixture
STUB_GH_ISSUE_ERR='HTTP 502 from roborev-defer: upstream connect error'
STUB_GH_COMMENTS="\001pmcfadin\n$d_grant\n"
run_wrapper "$w_work" --recheck-job 4656
assert_verdict 'case (df7j gh stderr)' FAIL 1
assert_says 'case (df7j gh stderr) the could-not-ask still names what it could not interpret' \
  "^deferral: ISSUE-UNVERIFIABLE \('gh issue view 3602' failed WITHOUT answering"
assert_says 'case (df7j gh stderr) with the keyword redacted by the WRAPPER boundary' \
  'HTTP 502 from \[authorization-keyword-redacted\]: upstream connect error'
assert_no_marker_form 'case (df7j gh stderr)'
assert_lacks 'case (df7j gh stderr) and nothing is deferred' '^findings: DEFERRED'
reset_stub
# AND AN ALLOWLISTED AUTHOR IS STILL UNTOUCHED: redaction is DISPLAY ONLY and the authorization decision
# is made on the RAW login before any renderer runs, so a normal login must reach the block verbatim.
# Without this control, a redaction that mangled every author would satisfy the two asserts above.
df_grant_fixture
STUB_GH_COMMENTS="\001pmcfadin\n$d_grant\n"
run_wrapper "$w_work" --recheck-job 4656
assert_verdict 'case (df7j control) an ordinary login is not redacted' PASS 0
assert_says 'case (df7j control) the granted author is recorded as GitHub gave it' \
  '^deferral: GRANTED \(author=@pmcfadin '
assert_lacks 'case (df7j control) no value was redacted on a clean run' 'authorization-keyword-redacted'
assert_no_marker_form 'case (df7j control)'
reset_stub

printf '== (df9) #3626: a gh failure (no PR / no auth / API error) is UNAVAILABLE and FAILs closed ==\n'
reset_stub
df_grant_fixture
STUB_GH_COMMENTS="\001pmcfadin\n$d_grant\n"
STUB_GH_RC=1
run_wrapper "$w_work" --recheck-job 4656
STUB_GH_RC=0
assert_verdict 'case (df9)' FAIL 1
assert_no_marker_form 'case (df9)'
assert_says 'case (df9) an unconsultable oracle is UNAVAILABLE, naming what could not be read' \
  "^deferral: UNAVAILABLE \(.gh pr view --json comments. failed"
assert_lacks 'case (df9) and nothing is deferred' '^findings: DEFERRED'
reset_stub

printf '== (df10) #3626: findings UNKNOWN is NOT deferrable, even with a granted-shaped marker ==\n'
# `UNKNOWN` means the findings state was never ESTABLISHED. We cannot count what we cannot see, so a
# deferral over it would be precisely "a pass resting on a state we could not read".
reset_stub
STUB_ANNOUNCE_SHA="$w_head"
STUB_PROMPT="$PROMPT_WITH_W_PATHS"
STUB_RECORD_OUTPUT='## Summary\\nsomething happened, in prose, with no severity marker.'
STUB_GH_ISSUES='3602 3613'
STUB_GH_COMMENTS="\001pmcfadin\n$d_grant\n"
run_wrapper "$w_work" --recheck-job 4656
assert_verdict 'case (df10)' FAIL 1
assert_no_marker_form 'case (df10)'
assert_says 'case (df10) the findings state is UNKNOWN' '^findings: UNKNOWN$'
assert_says 'case (df10) and UNKNOWN is refused as non-deferrable, in every mode' \
  '^deferral: UNAVAILABLE \(findings: UNKNOWN — the findings state was never ESTABLISHED'
assert_lacks 'case (df10) nothing is deferred' '^findings: DEFERRED'
reset_stub

printf '== (df10b) #3626: SKIP and a countless PRESENT are NOT deferrable (direct unit probe) ==\n'
# EXERCISED DIRECTLY because the wrapper cannot reach these states on a recheck: `findings:` is always
# assigned there, and a run that never reaches the check also never reaches the deferral. That is
# exactly why they are probed — a structural backstop must not depend on an upstream ordering still
# being there. A bare `PRESENT` carries NO count, so the affirmative half of the binding would be
# unenforceable, and only an affirmatively measured `PRESENT (n)` may be deferred.
df10b_probe="$tmp/df10b-probe.sh"
df10b_out="$tmp/df10b-out.txt"
cat >"$df10b_probe" <<'DF10B'
set -uo pipefail
. "$1"   # oracles
. "$2"   # checks
RECHECK_JOB=4656
JOB=4656
BASE=origin/main
LOG="$3/df10b.log"
for probe_value in SKIP PRESENT; do
  FINDINGS="$probe_value"
  DETAILS=()
  DEFERRAL_REPORT=""
  roborev_check_findings_deferral
  printf 'value=%s findings=%s deferral=%s\n' "$probe_value" "$FINDINGS" "$DEFERRAL_REPORT"
done
DF10B
if [ "$HAVE_PYTHON3" -eq 1 ] && bash "$df10b_probe" "$ORACLES_SRC" "$CHECKS_SRC" "$tmp" >"$df10b_out" 2>&1; then
  if grep -qE '^value=SKIP findings=SKIP deferral=UNAVAILABLE \(findings: SKIP — the findings state was never ESTABLISHED' "$df10b_out"; then
    ok 'case (df10b): findings SKIP is refused as non-deferrable and the value is left untouched'
  else
    bad "case (df10b): SKIP was not refused as non-deferrable: $(grep '^value=SKIP' "$df10b_out" || cat "$df10b_out")"
  fi
  if grep -qE '^value=PRESENT findings=PRESENT deferral=UNAVAILABLE \(findings: PRESENT carries no measured count' "$df10b_out"; then
    ok 'case (df10b): a countless PRESENT has no observed count to match, so it is refused'
  else
    bad "case (df10b): a countless PRESENT was not refused: $(grep '^value=PRESENT' "$df10b_out" || cat "$df10b_out")"
  fi
  if grep -q 'findings=DEFERRED' "$df10b_out"; then
    bad 'case (df10b): a non-deferrable findings state was rewritten to DEFERRED'
  else
    ok 'case (df10b): neither non-deferrable state can be rewritten to DEFERRED'
  fi
else
  bad "case (df10b): the unit probe did not run: $(cat "$df10b_out" 2>/dev/null)"
fi
reset_stub

printf '== (df11) #3626: SOLE-CONTENT refusals — indented, quoted, bulleted, mid-sentence ==\n'
# INHERITED BY CALL, NOT RE-DERIVED (#3312 job 29): four Markdown recognisers were superseded before
# this rule closed, and the deferral uses the SAME scanner rather than a second copy of it. These
# cases are the regression proof that the inheritance is real.
reset_stub
df_grant_fixture
STUB_GH_COMMENTS="\001pmcfadin\n> $d_grant\n    $d_grant\n- $d_grant\nthe form is $d_grant\n"
run_wrapper "$w_work" --recheck-job 4656
assert_verdict 'case (df11)' FAIL 1
assert_no_marker_form 'case (df11)'
assert_says 'case (df11) none of the four quoting shapes is an authorization' '^deferral: NONE \(no findings-deferral comment'
assert_lacks 'case (df11) and none of them grants' '^findings: DEFERRED'
reset_stub

printf '== (df11b) #3626: a FENCED and an HTML <pre>-wrapped marker are both inert ==\n'
# The two contexts the fence state machine got wrong: ```bash inside a fence is CONTENT, not a closing
# delimiter, and HTML <pre>/<code> was never handled at all. Under the sole-content rule neither needs
# handling — both are simply extra content.
reset_stub
df_grant_fixture
STUB_GH_COMMENTS_JSON=$(DF_GRANT="$d_grant" python3 -c '
import json, os
m = os.environ["DF_GRANT"]
fenced = "Here is the form:\n\n```bash\n" + m + "\n```"
html = "<pre><code>" + m + "</code></pre>"
print(json.dumps({"body": "links #3602 and #3613", "comments": [
    {"author": {"login": "pmcfadin"}, "body": fenced},
    {"author": {"login": "pmcfadin"}, "body": html}]}))
')
run_wrapper "$w_work" --recheck-job 4656
assert_verdict 'case (df11b)' FAIL 1
assert_no_marker_form 'case (df11b)'
assert_says 'case (df11b) documenting the form grants nothing' '^deferral: NONE \(no findings-deferral comment'
assert_lacks 'case (df11b) neither wrapper grants' '^findings: DEFERRED'
reset_stub

printf '== (df11c) #3626: the SANCTIONED workflow — commentary and authorization as SEPARATE comments ==\n'
# The cost of the rule, and the proof it is trivial: the lead explains in one comment (fenced example
# included) and authorizes in another. The marker-only comment grants; the documentation is inert.
reset_stub
df_grant_fixture
STUB_GH_COMMENTS_JSON=$(DF_GRANT="$d_grant" python3 -c '
import json, os
m = os.environ["DF_GRANT"]
doc = "Both findings are filed and deferred. The form is:\n\n```\n" + m + "\n```"
print(json.dumps({"comments": [
    {"author": {"login": "pmcfadin"}, "body": doc},
    {"author": {"login": "pmcfadin"}, "body": m}]}))
')
run_wrapper "$w_work" --recheck-job 4656
assert_verdict 'case (df11c)' PASS 0
assert_no_marker_form 'case (df11c)'
assert_says 'case (df11c) the marker-only comment grants' '^deferral: GRANTED \(author=@pmcfadin'
reset_stub

printf '== (df12) #3626: the failing block REPOSTED as a PR comment authorizes nothing ==\n'
# THE ARTIFACT MUST NOT BECOME THE CREDENTIAL (#3312 job 23), and the fixture IS the exploit: take the
# ENTIRE output of a failing run — block plus diagnostics — post it as a PR comment, and run again.
# Summary blocks are pasted into PR comments as a matter of course in this repository.
reset_stub
df_grant_fixture
run_wrapper "$w_work" --recheck-job 4656
assert_verdict 'case (df12) step 1: the un-deferred findings FAIL' FAIL 1
assert_no_marker_form 'case (df12) step 1: the un-deferred findings FAIL'
df12_paste="$tmp/df12-paste.txt"
{ printf '\001pmcfadin\n'; cat "$OUT"; } >"$df12_paste"
reset_stub
df_grant_fixture
STUB_GH_COMMENTS_FILE="$df12_paste"
run_wrapper "$w_work" --recheck-job 4656
assert_verdict 'case (df12)' FAIL 1
assert_no_marker_form 'case (df12)'
assert_says 'case (df12) the reposted diagnostic is not an authorization' '^deferral: NONE \(no findings-deferral comment'
assert_lacks 'case (df12) and it grants nothing' '^findings: DEFERRED'
reset_stub

printf '== (df13) #3626: SEPARATE SCOPING — an absence waiver confers NO authority over findings ==\n'
# A delivery-artifact waiver may never excuse a real defect. This is the pair that keeps the two
# authorizations from collapsing into one.
reset_stub
STUB_ANNOUNCE_SHA="$w_head"
STUB_PROMPT="$PROMPT_WITHOUT_PATHS"
STUB_VERDICT_FIELD='F'
STUB_RECORD_OUTPUT="$FINDINGS_TEXT"
STUB_GH_ISSUES='3602 3613'
STUB_GH_COMMENTS="\001pmcfadin\nroborev-waive: prompt-content-absent base=$w_base head=$w_head job=4656 reason=snapshot-delivered; 541812 in / 472576 cached\n"
run_wrapper "$w_work" --recheck-job 4656
assert_verdict 'case (df13)' FAIL 1
assert_no_marker_form 'case (df13)'
assert_says 'case (df13) the waiver still excuses exactly what it was authorized for' \
  '^prompt-content: WAIVED \(2/2 code census paths absent'
assert_says 'case (df13) but the findings are NOT deferred by it' '^findings: PRESENT \(2\)$'
assert_says 'case (df13) and the deferral key reports its own, independent state' \
  '^deferral: NONE \(no findings-deferral comment'
assert_lacks 'case (df13) a waiver can never carry a findings-bearing recheck to a PASS' '^RESULT: PASS$'
reset_stub

printf '== (df13b) #3626: SEPARATE SCOPING — a findings deferral confers NO authority over the prompt ==\n'
# The other direction, and the one a reader is likelier to assume: a granted deferral does its own job
# (findings: DEFERRED) while the absent census paths still FAIL, because no human waived THAT.
reset_stub
STUB_ANNOUNCE_SHA="$w_head"
STUB_PROMPT="$PROMPT_WITHOUT_PATHS"
STUB_VERDICT_FIELD='F'
STUB_RECORD_OUTPUT="$FINDINGS_TEXT"
STUB_GH_ISSUES='3602 3613'
STUB_GH_COMMENTS="\001pmcfadin\n$d_grant\n"
run_wrapper "$w_work" --recheck-job 4656
assert_verdict 'case (df13b)' FAIL 1
assert_no_marker_form 'case (df13b)'
assert_says 'case (df13b) the deferral does its own job' \
  '^findings: DEFERRED \(2, issues=#3602,#3613, authorized @pmcfadin, job 4656\)$'
assert_says 'case (df13b) and is recorded as granted' '^deferral: GRANTED \(author=@pmcfadin'
assert_says 'case (df13b) while the absent census paths still FAIL' \
  '^prompt-content: FAIL \(2/2 code census paths absent from the prompt\)$'
assert_says 'case (df13b) the waiver key reports its own independent state' '^waiver: NONE \(no waiver comment'
assert_lacks 'case (df13b) a deferral can never excuse another check' '^RESULT: PASS$'
reset_stub

printf '== (df13c) #3626: BOTH authorizations, each on its own marker, each under its own key ==\n'
# A run may legitimately carry both. Asserted because "separately scoped" must not degrade into
# "mutually exclusive": the failure mode of over-separating is as real as the failure mode of
# collapsing, and only this case distinguishes them.
reset_stub
STUB_ANNOUNCE_SHA="$w_head"
STUB_PROMPT="$PROMPT_WITHOUT_PATHS"
STUB_VERDICT_FIELD='F'
STUB_RECORD_OUTPUT="$FINDINGS_TEXT"
STUB_GH_ISSUES='3602 3613'
STUB_GH_COMMENTS="\001pmcfadin\nroborev-waive: prompt-content-absent base=$w_base head=$w_head job=4656 reason=snapshot-delivered; 541812 in / 472576 cached\n\001pmcfadin\n$d_grant\n"
run_wrapper "$w_work" --recheck-job 4656
assert_verdict 'case (df13c)' PASS 0
assert_no_marker_form 'case (df13c)'
assert_says 'case (df13c) the absence is WAIVED under its own key' '^prompt-content: WAIVED \(2/2 code census paths absent'
assert_says 'case (df13c) the findings are DEFERRED under theirs' '^findings: DEFERRED \(2, issues=#3602,#3613'
assert_says 'case (df13c) and each authorization is recorded separately' '^waiver: GRANTED \(author=@pmcfadin'
assert_says 'case (df13c) both keys speak' '^deferral: GRANTED \(author=@pmcfadin'
reset_stub

printf '== (df14) #3626: the scope binds the MERGE-BASE, not the base ref tip ==\n'
# THE STALENESS DEFECT #3392 FIXED, IN THE NEW BINDING. `<base>...HEAD` IS `merge-base..HEAD`, so an
# authorization bound to the base ref's TIP goes spuriously STALE the moment the base ref advances —
# which is what dead-lettered the #3312 break-glass under fleet load. The same mistake here would
# dead-letter this one on any branch whose main moved, i.e. almost every branch.
reset_stub
df14_work=$(make_fixture case_df14 advanced-base)
assert_base_advanced 'case (df14)' "$df14_work"
df14_tip=$(git -C "$df14_work" rev-parse origin/main)
df14_base=$(git -C "$df14_work" merge-base origin/main HEAD)
df14_head=$(git -C "$df14_work" rev-parse HEAD)
STUB_ANNOUNCE_SHA="$df14_base"
STUB_GIT_REF="$df14_base..$df14_head"
STUB_PROMPT="$PROMPT_WITH_PATHS"
STUB_VERDICT_FIELD='F'
STUB_RECORD_OUTPUT="$FINDINGS_TEXT"
STUB_GH_ISSUES='3602 3613'
STUB_GH_COMMENTS="\001pmcfadin\nroborev-defer: findings issues=$d_issues count=2 base=$df14_base head=$df14_head job=4656 reason=deferred against the reviewed range\n"
run_wrapper "$df14_work" --recheck-job 4656
assert_verdict 'case (df14)' PASS 0
assert_no_marker_form 'case (df14)'
assert_says 'case (df14) a marker naming the MERGE-BASE grants' \
  "^deferral: GRANTED \(author=@pmcfadin issues=$d_issues count=2 scope=base=$df14_base head=$df14_head job=4656 reason=deferred against the reviewed range\)\$"
assert_says 'case (df14) and the findings are DEFERRED' '^findings: DEFERRED \(2, issues=#3602,#3613'
reset_stub

printf '== (df14b) #3626: a marker naming the STALE base ref TIP does NOT grant (the control) ==\n'
# Only `base=` differs from (df14) — it names a REAL commit that is not the base of the reviewed range
# — so the marker names a different review. This is what makes (df14) a measurement of the binding
# rather than of the parser.
reset_stub
STUB_ANNOUNCE_SHA="$df14_base"
STUB_GIT_REF="$df14_base..$df14_head"
STUB_PROMPT="$PROMPT_WITH_PATHS"
STUB_VERDICT_FIELD='F'
STUB_RECORD_OUTPUT="$FINDINGS_TEXT"
STUB_GH_ISSUES='3602 3613'
STUB_GH_COMMENTS="\001pmcfadin\nroborev-defer: findings issues=$d_issues count=2 base=$df14_tip head=$df14_head job=4656 reason=bound to the wrong base\n"
run_wrapper "$df14_work" --recheck-job 4656
assert_verdict 'case (df14b)' FAIL 1
assert_no_marker_form 'case (df14b)'
assert_says 'case (df14b) the tip-bound marker is STALE' \
  "^deferral: STALE \(the marker names a different review — base \($df14_tip != $df14_base\)"
assert_lacks 'case (df14b) and it grants nothing' '^findings: DEFERRED'
reset_stub

printf '== (df15) #3626: a deferral is NOT applied to a FRESH review, and no key is emitted ==\n'
# RECHECK-ONLY is a property of the mechanism, not a convenience: the authorizer learns the job id AND
# the findings from the FINISHED run, so a fresh run enqueues a DIFFERENT job than any marker names. A
# fresh review with open findings therefore still FAILs under the reviewer's own exit status, and the
# `deferral:` key is ABSENT rather than placeholdered — there was no deferral to look for.
reset_stub
STUB_ANNOUNCE_SHA="$w_head"
STUB_PROMPT="$PROMPT_WITH_W_PATHS"
STUB_VERDICT=$'## Findings\n- **Severity**: High\nProblem: the first one.\n- **Severity**: Medium\nProblem: the second one.\n## Summary\n2 findings.'
STUB_REVIEW_RC=1
STUB_GH_ISSUES='3602 3613'
STUB_GH_COMMENTS="\001pmcfadin\n$d_grant\n"
run_wrapper "$w_work"
assert_verdict 'case (df15)' FAIL 1
assert_no_marker_form 'case (df15)'
assert_says 'case (df15) a reviewer that RAN reports its exit honestly' '^roborev-exit: FINDINGS \(exit 1\)$'
assert_says 'case (df15) the findings are reported present' '^findings: PRESENT'
assert_lacks 'case (df15) no deferral is applied to a fresh review' '^findings: DEFERRED'
assert_lacks 'case (df15) and the key has no subject, so it is absent' '^deferral:'
reset_stub

printf '== (df16) #3626: a DEFERRED token with NO granted deferral FAILs (mutant) ==\n'
# THE COUPLING, ASSERTED AGAINST A HOSTILE CODE PATH. `DEFERRED` is non-failing ONLY on the coupled
# granted state, so a future check that emits the token having measured nothing must not ride to a
# PASS — which is the exact shape (#3229/#3586) of a verdict resting on the absence of a bad signal.
# THE CONTROL RUNS FIRST AND IS NOT OPTIONAL: an assert that a patched copy FAILs is satisfied by a
# copy that fails because it was copied wrong, which is a probe failing in the direction that looks
# like success.
reset_stub
_df_dir="$tmp/deferral-mutant"
mkdir -p "$_df_dir"
cp "$WRAPPER_REAL" "$SCRIPT_DIR/../flow/roborev-review-oracles.sh" \
  "$SCRIPT_DIR/../flow/roborev-review-checks.sh" "$SCAN_TOOL" "$_df_dir/"
if [ -f "$SCRIPT_DIR/../flow/roborev-job-facts.py" ]; then
  cp "$SCRIPT_DIR/../flow/roborev-job-facts.py" "$_df_dir/"
fi
STUB_ANNOUNCE_SHA="$w_head"
STUB_PROMPT="$PROMPT_WITH_W_PATHS"
STUB_VERDICT_FIELD='P'
STUB_RECORD_OUTPUT="$CLEAN_TEXT"
run_wrapper --wrapper "$_df_dir/roborev-review.sh" "$w_work" --recheck-job 4656
assert_verdict 'case (df16 control) the UNPATCHED copy reaches PASS on a clean recheck' PASS 0
assert_no_marker_form 'case (df16 control) the UNPATCHED copy reaches PASS on a clean recheck'
assert_says 'case (df16 control) with an affirmative NONE and no deferral key' '^findings: NONE$'
assert_lacks 'case (df16 control) and no deferral was looked for' '^deferral:'
if sed_inplace_verified "$_df_dir/roborev-review-checks.sh" \
  's/^roborev_check_findings_deferral() {$/roborev_check_findings_deferral() {\
  FINDINGS="DEFERRED (2, issues=#1, authorized @nobody, job 4656)"\
  DEFERRAL_REPORT="GRANTED (fabricated by a code path that measured nothing)"\
  return 0/' \
  'fabricated by a code path that measured nothing' ''; then
  ok 'case (df16): the fabricated-DEFERRED patch was really applied to the copy'
  run_wrapper --wrapper "$_df_dir/roborev-review.sh" "$w_work" --recheck-job 4656
  assert_verdict 'case (df16)' FAIL 1
  assert_no_marker_form 'case (df16)'
  assert_says 'case (df16) the ungranted DEFERRED is named under its own diagnostic' \
    '^ERROR: verdict-grammar: a per-check key reports a DEFERRED state that the deferral oracle did not affirmatively GRANT'
  assert_says 'case (df16) it states what a grant requires' 'whose authorized count EQUALS the count this run observed'
  assert_says 'case (df16) and why an unbacked token is refused' \
    'indistinguishable from an authorized one to every reader of this block'
  assert_lacks 'case (df16) a fabricated deferral can never reach a PASS' '^RESULT: PASS$'
else
  bad 'case (df16): could not patch the copied checks file, so the ungranted-DEFERRED path was never exercised (a green run here would be a probe failing in the direction that looks like success)'
fi
reset_stub

printf '== (df17) #3626: the verdict token is matched EXACTLY — DEFERREDX is unrecognised ==\n'
# A `DEFERRED*` prefix glob would accept `DEFERREDX` and check a SPELLING rather than a STATE, which
# is the closure reopening the hole it exists to shut, one level down (#3229).
if sed_inplace_verified "$_df_dir/roborev-review-checks.sh" \
  's/  FINDINGS="DEFERRED (2, issues=#1, authorized @nobody, job 4656)"/  FINDINGS="DEFERREDX (2, issues=#1, authorized @nobody, job 4656)"/' \
  'FINDINGS="DEFERREDX (2' 'FINDINGS="DEFERRED (2'; then
  ok 'case (df17): the near-miss-token patch was really applied to the copy'
  run_wrapper --wrapper "$_df_dir/roborev-review.sh" "$w_work" --recheck-job 4656
  assert_verdict 'case (df17)' FAIL 1
  assert_no_marker_form 'case (df17)'
  assert_says 'case (df17) the near-miss token is UNRECOGNISED, not accepted as DEFERRED' \
    "^ERROR: verdict-grammar: a per-check key holds a value outside the block's documented grammar: 'DEFERREDX "
  assert_lacks 'case (df17) and it never reaches a PASS' '^RESULT: PASS$'
else
  bad 'case (df17): could not patch the copied checks file for the near-miss token, so token-exactness was never exercised'
fi
reset_stub

printf '== (df18) #3626: a GRANTED deferral does NOT excuse a NON-findings key (mutant) ==\n'
# THE CONFINEMENT, ASSERTED AGAINST A HOSTILE CODE PATH (roborev job 225; spec: "the deferral SHALL
# NOT be readable by, or applicable to, any check other than the wrapper's `findings:` key", and "a
# findings deferral SHALL confer no authority over `prompt-content:`"). The wrapper used to admit
# `DEFERRED` from the coupled granted state for ANY key, so ONE authorization — over a named set of
# findings, which says nothing about whether the reviewer's diff ever arrived — would have excused
# `prompt-content:` too. It was not reachable at the time, because no other key emitted the token;
# that is precisely the latent false pass of #3564 (delegating a key's failure to its neighbour), so
# it is pinned here against a key that DOES emit it.
#
# THE CONTROL RUNS FIRST AND IS NOT OPTIONAL, and here it carries the whole weight of the case: the
# UNPATCHED copy must reach PASS on this very fixture (a granted, matching deferral), so the FAIL
# below cannot be the copy failing for some unrelated reason — the ONLY difference between the two
# runs is which key carries the token.
reset_stub
_dfk_dir="$tmp/deferral-confinement"
mkdir -p "$_dfk_dir"
cp "$WRAPPER_REAL" "$SCRIPT_DIR/../flow/roborev-review-oracles.sh" \
  "$SCRIPT_DIR/../flow/roborev-review-checks.sh" "$SCAN_TOOL" "$_dfk_dir/"
if [ -f "$SCRIPT_DIR/../flow/roborev-job-facts.py" ]; then
  cp "$SCRIPT_DIR/../flow/roborev-job-facts.py" "$_dfk_dir/"
fi
df_grant_fixture
STUB_GH_COMMENTS="\001pmcfadin\n$d_grant\n"
run_wrapper --wrapper "$_dfk_dir/roborev-review.sh" "$w_work" --recheck-job 4656
assert_verdict 'case (df18 control) the UNPATCHED copy reaches PASS on the granted deferral' PASS 0
assert_no_marker_form 'case (df18 control) the UNPATCHED copy reaches PASS on the granted deferral'
assert_says 'case (df18 control) with the findings deferred under their own key' \
  '^findings: DEFERRED \(2, issues=#3602,#3613'
assert_says 'case (df18 control) and prompt-content affirmatively PASSing' \
  '^prompt-content: PASS \(2/2 code census paths present\)$'
if sed_inplace_verified "$_dfk_dir/roborev-review-checks.sh" \
  's/^roborev_check_prompt_content() {$/roborev_check_prompt_content() {\
  PROMPT_CONTENT="DEFERRED (a key other than findings claiming the token)"\
  return 0/' \
  'a key other than findings claiming the token' ''; then
  ok 'case (df18): the misplaced-DEFERRED patch was really applied to the copy'
  run_wrapper --wrapper "$_dfk_dir/roborev-review.sh" "$w_work" --recheck-job 4656
  assert_verdict 'case (df18)' FAIL 1
  assert_no_marker_form 'case (df18)'
  assert_says 'case (df18) the misplaced token is named under its own diagnostic, WITH its key' \
    "^ERROR: verdict-grammar: a per-check key OTHER THAN 'findings:' reports a DEFERRED state — prompt-content: 'DEFERRED "
  assert_says 'case (df18) the diagnostic states the authorization scope a deferral actually has' \
    "confers authority over the 'findings:' key and nothing else"
  assert_says 'case (df18) and that a granted deferral changes nothing about it' \
    'This holds even when a deferral WAS granted, and it is not waivable'
  # THE GRANT ITSELF IS UNAFFECTED — separate scoping in the direction (df13/df13b) cover from the
  # marker side, here from the VERDICT side: the deferral still does its own job under its own key.
  assert_says 'case (df18) the findings are still deferred under their own key' \
    '^findings: DEFERRED \(2, issues=#3602,#3613'
  assert_lacks 'case (df18) one authorization can never excuse a check nobody authorized' '^RESULT: PASS$'
else
  bad 'case (df18): could not patch the copied checks file, so the misplaced-DEFERRED path was never exercised (a green run here would be a probe failing in the direction that looks like success)'
fi
reset_stub

printf '== case (mb9): the ENQUEUED range is IMMUTABLE against a mid-review base-ref move ==\n'
# THE RESIDUAL SECOND-ORDER RACE, CLOSED BY CONSTRUCTION (roborev round 1, Medium). The wrapper used
# to pass the SYMBOLIC base ref to `roborev review --base`, so roborev re-resolved the mirror ref
# ITSELF and computed its own merge-base. A ref move between the census and the enqueue therefore
# reviewed a DIFFERENT range than the census measured, and the only thing that noticed was
# `sha-assert` — AFTER a full-price review had been spent. Passing the RESOLVED sha makes the
# divergence unexpressible instead of merely detectable.
#
# WHAT THIS CASE CAN AND CANNOT MEASURE, stated rather than implied. The stub reviewer cannot
# re-derive a range from `--base` the way the real binary does, so this case does NOT measure
# roborev's internal resolution (that is measured against the REAL binary, recorded in the wrapper's
# comment at the enqueue: `--base <sha>` and `--base origin/main` produced the IDENTICAL
# merge-base-anchored `git_ref` on a repo whose main had advanced). What it DOES measure, hermetically
# and decisively, is the property the fix rests on: the argument the wrapper hands to the enqueue, and
# the base its assert later compares against, are BOTH the sha captured by the census and are
# UNAFFECTED by the ref moving mid-review.
#
# THE MOVE IS CHOSEN TO CHANGE THE MERGE-BASE, not just the tip: it force-pushes the feature branch
# onto main, after which `merge-base(origin/main, HEAD)` is HEAD itself. So a wrapper that passed the
# symbolic ref would have had roborev resolve a demonstrably different range, and a wrapper that
# re-read the base after the review would assert against a different sha. Both are asserted against.
reset_stub
work=$(make_fixture case_mb9 advanced-base)
assert_base_advanced 'case (mb9)' "$work"
mb9_base=$(git -C "$work" merge-base origin/main HEAD)
mb9_head=$(git -C "$work" rev-parse HEAD)
STUB_ANNOUNCE_SHA="$mb9_base"
STUB_GIT_REF="$mb9_base..$mb9_head"
# Force-push the branch onto the remote's main, then update the mirror ref: the census has already
# run, so this lands strictly between the census and the assert.
STUB_ON_REVIEW="git -C '$work' push -q -f origin feature:main && git -C '$work' fetch -q origin"
run_wrapper "$work"
STUB_ON_REVIEW=''
# NON-VACUITY, in the direction that matters: the mid-review move must really have changed what
# `origin/main` denotes AND what its merge-base with HEAD would now be. If it did not, this case
# proves nothing about immutability.
mb9_moved_base=$(git -C "$work" merge-base origin/main HEAD)
if [ "$mb9_moved_base" != "$mb9_base" ]; then
  ok "case (mb9): the mid-review move really changed the merge-base ($mb9_base -> $mb9_moved_base), so the pinning is load-bearing here"
else
  bad "case (mb9): the mid-review move did NOT change the merge-base (still $mb9_base) — the case cannot distinguish a pinned base from a re-resolved one"
fi
assert_verdict 'case (mb9)' PASS 0
assert_says 'case (mb9) sha-assert still PASSes against the PRE-MOVE range' '^sha-assert: PASS$'
assert_says 'case (mb9) assert-base still names the census-time merge-base' "^assert-base: $mb9_base "
assert_lacks 'case (mb9) the post-move merge-base is not what the assert used' "^assert-base: $mb9_moved_base "
if grep -qF -- "--base $mb9_base " "$INVOKED"; then
  ok 'case (mb9): the enqueue carried the census-time merge-base SHA, so the ref move could not change the reviewed range'
else
  bad "case (mb9): the enqueue did not carry the census-time merge-base '$mb9_base': $(cat "$INVOKED")"
fi
if grep -qF -- '--base origin/main' "$INVOKED"; then
  bad 'case (mb9): the SYMBOLIC base ref reached the enqueue, so roborev would have re-resolved it AFTER the move — the race is still open'
else
  ok 'case (mb9): no symbolic base ref reaches the enqueue'
fi

printf '== case (mb8): the absence WAIVER is bound to the MERGE-BASE, not the base ref tip ==\n'
# THE SAME STALENESS DEFECT, IN THE WAIVER (#3392). The waiver scope is `base=<sha> head=<sha>
# job=<id>` and it identifies THE REVIEWED RANGE, whose base is the merge-base. Bound to the base
# ref's TIP instead, a waiver written for a failing run went spuriously STALE on `--recheck-job`
# the moment the base ref advanced — re-deadlettering the #3312 break-glass exactly when the fleet
# is busiest. Nothing is weakened here: base AND head AND job are all still required and verified.
reset_stub
mb8_work=$(make_fixture case_mb8 advanced-base)
assert_base_advanced 'case (mb8)' "$mb8_work"
mb8_tip=$(git -C "$mb8_work" rev-parse origin/main)
mb8_base=$(git -C "$mb8_work" merge-base origin/main HEAD)
mb8_head=$(git -C "$mb8_work" rev-parse HEAD)
STUB_ANNOUNCE_SHA="$mb8_base"
STUB_GIT_REF="$mb8_base..$mb8_head"
STUB_PROMPT="$PROMPT_WITHOUT_PATHS"
STUB_GH_COMMENTS="\001pmcfadin\nroborev-waive: prompt-content-absent base=$mb8_base head=$mb8_head job=4656 reason=absence checked against the token accounting\n"
run_wrapper "$mb8_work"
# THE TERMINAL VERDICT, not just the two waiver lines (roborev round 1, Low). Without it a regression
# in the waiver-ADMISSION path — the affirmation backstop refusing a `WAIVED` whose provenance it can
# no longer match — would leave this case green while the waiver was unusable in practice: the two
# asserts below only prove the waiver was FOUND and RECORDED, never that it let the run reach a pass.
assert_verdict 'case (mb8)' PASS 0
assert_says 'case (mb8) a marker naming the MERGE-BASE grants' \
  "^waiver: GRANTED \(author=@pmcfadin base=$mb8_base head=$mb8_head job=4656 reason=absence checked against the token accounting\)\$"
assert_says 'case (mb8) and the absence verdict is WAIVED, not PASS' '^prompt-content: WAIVED \(1/1 code census paths absent'
reset_stub

printf '== case (mb8b): a marker naming the STALE base ref TIP does NOT grant ==\n'
# THE CONTROL. Only the `base=` field differs from (mb8) — it names the base ref's TIP, which is a
# real commit but NOT the base of the reviewed range — so the marker names a DIFFERENT review and
# the FAIL stands. This is what makes (mb8) a measurement of the binding rather than of the parser.
reset_stub
STUB_ANNOUNCE_SHA="$mb8_base"
STUB_GIT_REF="$mb8_base..$mb8_head"
STUB_PROMPT="$PROMPT_WITHOUT_PATHS"
STUB_GH_COMMENTS="\001pmcfadin\nroborev-waive: prompt-content-absent base=$mb8_tip head=$mb8_head job=4656 reason=bound to the wrong base\n"
run_wrapper "$mb8_work"
assert_verdict 'case (mb8b)' FAIL 1
assert_says 'case (mb8b) the tip-bound marker is STALE' \
  "^waiver: STALE \(the marker names a different review — base \($mb8_tip != $mb8_base\)"
assert_lacks 'case (mb8b) and it grants nothing' '^prompt-content: WAIVED'
assert_says 'case (mb8b) the absence FAIL stands' \
  '^prompt-content: FAIL \(1/1 code census paths absent from the prompt\)$'
reset_stub

# ============================================================================
# (jm*) #3654: ROBOREV JOB IDS ARE PER-DAEMON, AND THE BLOCK SAYS WHICH DAEMON
# ----------------------------------------------------------------------------
# THE INCIDENT: two lanes on two boxes requested absence waivers 50 minutes apart, both
# naming `job=265` for DIFFERENT reviews (different ranges, branches and token counts).
# Both were correct — ids are sequential PER DAEMON — but the coordination lead read the
# repetition as a collision and WITHHELD a valid waiver. So the block now NAMES the
# issuing daemon beside the id it disambiguates.
#
# THE KEY IS INFORMATIONAL AND THAT IS THE PROPERTY MOST WORTH PINNING: it is in neither
# the verdict-grammar scan nor the affirmation backstop, so none of its three states can
# red an otherwise-clean run. A decorative key that can fail a correct review is the guard
# agents learn to waive.
JM_UUID='db07281a-b8e0-4c8f-99dc-37ca88a0a54c'
jm_work=$(make_fixture case_jm pushed)
jm_head=$(git -C "$jm_work" rev-parse HEAD)
# The three renderings OBSERVED by the cases below, collected for the closed-set assert
# (jm6). Collected rather than predicted: a closed set asserted against literals someone
# typed proves only that the literals were typed.
jm_seen_uuid=''
jm_seen_not_recorded=''
jm_seen_unavailable=''
jm_render() { grep -E '^job-machine: ' "$OUT" | head -1 || printf ''; }

printf '== (jm1) #3654: the daemon id is surfaced even when `show` alone answered the record ==\n'
# THE CASE THAT MAKES THE KEY MORE THAN DECORATION. `read_job_record` STOPS at the first payload
# that carries the required fields, and on a healthy daemon that is `show` — which does not carry
# `source_machine_id` at all. Read only through that loop the key would be `NOT RECORDED` on EVERY
# real run: a key reporting nothing while looking like it reports something. So the wrapper takes
# one supplementary `list` read, and this case is the only thing that proves it happens.
reset_stub
STUB_ANNOUNCE_SHA="$jm_head"
STUB_SOURCE_MACHINE_ID="$JM_UUID"
run_wrapper "$jm_work"
assert_verdict 'case (jm1)' PASS 0
assert_says 'case (jm1) the block names the issuing daemon' "^job-machine: $JM_UUID \(source_machine_id; job ids are per-daemon\)$"
assert_says 'case (jm1) it sits beside the id it disambiguates' '^job: 4656$'
jm_seen_uuid=$(jm_render)
reset_stub

printf '== (jm2) #3654: the same id when the RECORD ITSELF came from the `list` payload ==\n'
# The other route to the fact: a `show` that returns the REVIEW row (no git_ref) makes the record
# loop fall through to `list`, so `source_machine_id` is already in the facts file and no
# supplementary read is needed. Both routes must render identically — otherwise the value a reader
# sees would depend on which payload happened to answer.
reset_stub
STUB_ANNOUNCE_SHA="$jm_head"
STUB_SOURCE_MACHINE_ID="$JM_UUID"
STUB_SHOW_JSON=review-row
run_wrapper "$jm_work"
assert_verdict 'case (jm2)' PASS 0
assert_says 'case (jm2) the daemon id is the same on the record-from-list route' "^job-machine: $JM_UUID \(source_machine_id; job ids are per-daemon\)$"
reset_stub

printf '== (jm3) #3654: NO daemon id anywhere is NOT RECORDED — and still PASSes ==\n'
# A REAL state, not a defensive one: a roborev build whose payloads carry no `source_machine_id`
# reaches it on every run. It must therefore be AFFIRMATIVE (never blank, never a bare `-`) and it
# must never cost a correct review its PASS — this case asserts both at once.
reset_stub
STUB_ANNOUNCE_SHA="$jm_head"
run_wrapper "$jm_work"
assert_verdict 'case (jm3) an unrecorded daemon never fails a clean run' PASS 0
# THE CAUSE IS THE ONE THIS RUN MEASURED, not a generic sentence: the record was read, the daemon's
# list for the job's OWN branch was consulted, and neither carried the field. Asserted with the tail
# that names WHICH payload carries it, because a cause without a remedy is half a diagnostic.
assert_says 'case (jm3) the state names the cause it actually measured' \
  "^job-machine: NOT RECORDED \(neither the job record nor the daemon's job list for branch 'feature' carries source_machine_id;"
assert_says 'case (jm3) and still names the payload that DOES carry the field' \
  "'roborev list --json' rows carry that field, 'roborev show <id> --json' does not\. Identify the review by the record's git_ref, never by the id alone\)$"
assert_lacks 'case (jm3) the key is never blank' '^job-machine: *$'
jm_seen_not_recorded=$(jm_render)
reset_stub

printf '== (jm4) #3654: the key is emitted in --recheck-job mode too ==\n'
# THE MODE THAT MATTERS MOST: `--recheck-job` is the only path an absence waiver travels, and the
# waiver names a job id — so this is precisely where a reader has to know WHICH daemon issued it.
reset_stub
STUB_ANNOUNCE_SHA="$jm_head"
STUB_SOURCE_MACHINE_ID="$JM_UUID"
STUB_VERDICT_FIELD='P'
STUB_RECORD_OUTPUT="$CLEAN_TEXT"
run_wrapper "$jm_work" --recheck-job 4656
assert_verdict 'case (jm4)' PASS 0
assert_says 'case (jm4) a recheck names the issuing daemon' "^job-machine: $JM_UUID \(source_machine_id; job ids are per-daemon\)$"
assert_says 'case (jm4) and the recheck mode is still declared' '^MODE: recheck '
reset_stub

printf '== (jm5) #3654: NO readable record at all is UNAVAILABLE, naming what it inherits ==\n'
# Distinct from (jm3) BECAUSE THEY ARE DIFFERENT OPERATOR ACTIONS: "this roborev build does not
# record the field" and "no record could be read" send a reader to different places. The run FAILs
# here — sha-assert cannot verify a range with no record — and the assert below pins that the FAIL
# is NOT attributed to this key.
reset_stub
STUB_ANNOUNCE_SHA="$jm_head"
STUB_SHOW_JSON=none
STUB_LIST_JSON=none
run_wrapper "$jm_work"
assert_verdict 'case (jm5)' FAIL 1
assert_says 'case (jm5) the key names the job-record state it inherits' \
  '^job-machine: UNAVAILABLE \(no job record could be read at all, so the issuing daemon is unknown — job-record: DEGRADED'
assert_says 'case (jm5) the FAIL belongs to sha-assert, not to job-machine' \
  '^sha-assert: FAIL \(job record unavailable'
assert_lacks 'case (jm5) no diagnostic blames the informational key' 'ERROR: job-machine'
jm_seen_unavailable=$(jm_render)
reset_stub

printf '== (jm6) #3654: the renderings are a CLOSED SET OF THREE, matched on the state token ==\n'
# PINNED AS A SET, NOT AS ONE LITERAL, which is the idiom this repo already uses for the gate's
# component-set `src_note`: pinning ONE literal reds on correct input (which arm fires depends on
# which payload answered), and pinning NOTHING lets a wording pass delete the key. So all three
# observed values are reduced to their STATE TOKEN and the set is compared by string equality.
jm_token() { # jm_token <rendered "job-machine: <value>" line> -> the state token
  local v="${1#job-machine: }"
  case "$v" in
    "$JM_UUID "*) printf '<uuid>' ;;
    'NOT RECORDED ('*) printf 'NOT RECORDED' ;;
    'UNAVAILABLE ('*) printf 'UNAVAILABLE' ;;
    '') printf '<absent>' ;;
    *) printf '<unrecognised:%s>' "${v%% *}" ;;
  esac
}
jm_set="$(jm_token "$jm_seen_uuid")|$(jm_token "$jm_seen_not_recorded")|$(jm_token "$jm_seen_unavailable")"
if [ "$jm_set" = '<uuid>|NOT RECORDED|UNAVAILABLE' ]; then
  ok 'case (jm6) the observed renderings are exactly the closed set {<uuid>, NOT RECORDED, UNAVAILABLE}'
else
  bad "case (jm6) the job-machine: renderings are not the closed set of three: observed '$jm_set' (expected '<uuid>|NOT RECORDED|UNAVAILABLE')"
fi
# AND EVERY ONE OF THEM CARRIES A REASON, never a bare token: an operator reading a pasted block
# has to be able to act on it without opening this file.
for _jm_r in "$jm_seen_uuid" "$jm_seen_not_recorded" "$jm_seen_unavailable"; do
  case "$_jm_r" in
    'job-machine: '*' ('*')'*) ok "case (jm6) the rendering carries its own explanation: ${_jm_r%% (*} (...)" ;;
    *) bad "case (jm6) a job-machine: rendering carries no parenthesised explanation: '$_jm_r'" ;;
  esac
done

printf '== (jm7) #3654 structural: job-machine is in NEITHER failing-capable key set ==\n'
# A BEHAVIOURAL CASE ONLY COVERS THE VALUES SOMEONE THOUGHT OF. (jm3)/(jm5) show that the two
# non-uuid states do not red a run TODAY; only a structural assert stops a future edit registering
# the key in the verdict scan or the affirmation backstop, where a `NOT RECORDED` — a routine state
# on a roborev build that does not carry the field — would fail every review on the fleet.
# Read from the two STATEMENTS, never from a file-wide grep: the key's own `emit_kv` line would
# satisfy a file-wide search even with the key registered in both scans.
_jm_scan_keys=$(sed -n '/^[[:space:]]*for scan_keyed in /,/; do$/p' "$WRAPPER_REAL")
_jm_aff_keys=$(sed -n '/^[[:space:]]*for keyed in "push-assert=/,/; do$/p' "$WRAPPER_REAL")
if [ -z "$_jm_scan_keys" ] || [ -z "$_jm_aff_keys" ]; then
  bad 'case (jm7) could not extract the verdict-scan and affirmation key lists to inspect'
else
  if printf '%s\n' "$_jm_scan_keys" | grep -q 'job-machine'; then
    bad 'case (jm7) job-machine is registered in the failing-capable verdict scan — an informational key would then red a correct run whenever the daemon id is NOT RECORDED (a routine state)'
  else
    ok 'case (jm7) job-machine is absent from the failing-capable verdict scan'
  fi
  if printf '%s\n' "$_jm_aff_keys" | grep -q 'job-machine'; then
    bad 'case (jm7) job-machine is registered in the affirmation backstop — a PASS would then require it to read PASS, which it never does'
  else
    ok 'case (jm7) job-machine is absent from the affirmation backstop'
  fi
fi
# AND IT IS ACTUALLY EMITTED, unconditionally. Without this the two asserts above are satisfied by
# DELETING the key, which is the decorative-assert trap this suite has ruled on before.
# The VALUE EXPRESSION is deliberately not pinned: since #3654 round 3 the fallback is built at
# emit time (`$(job_machine_value)`) rather than captured at initialization, so pinning the old
# `"$JOB_MACHINE"` spelling would red on correct code. What must hold is unchanged and is what is
# asserted — the key goes through `emit_kv`, the ONE neutralisation boundary, unconditionally.
if grep -qE "^[[:space:]]*emit_kv 'job-machine' \"[^\"]+\"$" "$WRAPPER_REAL"; then
  ok 'case (jm7) the key is emitted through the ONE value boundary (emit_kv), unconditionally'
else
  bad 'case (jm7) job-machine is not emitted via emit_kv — either it is gone, or its value bypasses the neutralisation boundary'
fi

printf '== (jm8) #3654 structural: NO machine field was added to either marker grammar ==\n'
# ASK 4 OF THE ISSUE, PINNED. The authorizer would have to know the value, it is derivable from the
# record, and every field in a hand-typed control line is one more way for a legitimate
# authorization to read MALFORMED. The two marker patterns are the grammar, so they are the subject.
_jm_scan_py="$SCRIPT_DIR/../flow/roborev-waiver-scan.py"
if [ ! -f "$_jm_scan_py" ]; then
  bad 'case (jm8) the waiver scanner is missing, so the marker grammar could not be inspected'
else
  _jm_patterns=$(grep -nE '^[[:space:]]*r"' "$_jm_scan_py" || printf '')
  if [ -z "$_jm_patterns" ]; then
    bad 'case (jm8) no marker pattern fragments found in the scanner — the extraction is stale'
  elif printf '%s\n' "$_jm_patterns" | grep -qiE 'machine'; then
    bad 'case (jm8) a machine field appears in a marker pattern — the marker grammar must stay unchanged (#3654 ask 4)'
  else
    ok 'case (jm8) neither marker pattern names a machine field — the grammar is unchanged'
  fi
fi

printf '== (jm9) #3654: the facts tool extracts source_machine_id from `list`, and NOTHING from `show` ==\n'
# THE PAYLOAD ASYMMETRY AT ITS SOURCE. A `show`-shaped payload must yield NO fact rather than an
# empty-string one: `source_machine_id=` in the facts file would render as a BLANK uuid, which is
# the unmeasured-value-as-a-value defect this repository fails runs for.
_jm_tool="$SCRIPT_DIR/../flow/roborev-job-facts.py"
_jm_facts="$tmp/jm-facts.txt"
# ===== SUB-CASES SHARE ONE FACTS FILE, SO EACH RUN MUST START FROM AN EMPTY ONE (#3654 r3) =====
# Every sub-case below ran the tool `|| true` against the SAME `$_jm_facts`. A tool failure
# therefore left the PREVIOUS sub-case's output in place and the next assert read it — jm11's
# `git_ref == aaa..bbb` could pass on jm9's stale file, a green that measured nothing. The tally
# cannot see that, which is the point. Truncating is necessary and NOT sufficient: the sub-cases
# asserting an ABSENCE (no `source_machine_id`) are satisfied trivially by an empty file, so the
# tool is also required to have SUCCEEDED and to have produced output.
# ===== AND IT MUST RUN IN THIS SHELL, NOT AS A PIPELINE COMPONENT (#3654 round 3) =====
# `printf ... | _jm_run 4656` puts the function in a SUBSHELL, so its `bad` calls incremented a
# COPY of FAIL and the verdict was discarded on exit — the tally stayed green while the guard
# reported a failure into the void. That is CLAUDE.md's rule stated for cargo parsers and general
# in fact: READ BY REDIRECTION, NEVER A PIPE, because a piped reader runs in a subshell and its
# verdict is thrown away. The payload is therefore an ARGUMENT written to a file and fed by `<`.
_jm_payload_file="$tmp/jm-payload.json"
_jm_run() { # _jm_run <job> <payload> — runs in THIS shell; payload by redirection, never a pipe
  printf '%s' "$2" >"$_jm_payload_file"
  : >"$_jm_facts"
  : >"$_jm_prompt"
  if ! python3 "$_jm_tool" "$1" "$_jm_facts" "$_jm_prompt" <"$_jm_payload_file" >/dev/null 2>&1; then
    bad "case (jm9/jm11) the facts tool exited non-zero for job $1 — the assert that follows would read an empty or stale facts file"
    return 1
  fi
  if [ ! -s "$_jm_facts" ]; then
    bad "case (jm9/jm11) the facts tool produced NO facts for job $1 — an absence assert would pass vacuously"
    return 1
  fi
  return 0
}
_jm_prompt="$tmp/jm-facts-prompt.txt"
if [ ! -f "$_jm_tool" ] || ! command -v python3 >/dev/null 2>&1; then
  printf 'SKIP - roborev-job-facts.py or python3 unavailable; the fact-extraction cases did not run\n'
else
  _jm_run 4656 "$(printf '[{"id":4656,"git_ref":"aaa..bbb","status":"done","source_machine_id":"%s"}]' "$JM_UUID")" || true
  if [ "$(sed -n 's/^source_machine_id=//p' "$_jm_facts" | head -1)" = "$JM_UUID" ]; then
    ok 'case (jm9) a list-shaped row yields source_machine_id as a string fact'
  else
    bad "case (jm9) the list-shaped row did not yield source_machine_id (facts: $(tr '\n' ' ' <"$_jm_facts"))"
  fi
  _jm_run 4656 "$(printf '{"id":4656,"job_id":4656,"agent":"codex","prompt":"p","job":{"id":4656,"git_ref":"aaa..bbb","status":"done"}}')" || true
  if grep -q '^source_machine_id=' "$_jm_facts"; then
    bad "case (jm9) a show-shaped payload emitted a source_machine_id fact — an empty one renders as a blank uuid (facts: $(tr '\n' ' ' <"$_jm_facts"))"
  else
    ok 'case (jm9) a show-shaped payload yields NO source_machine_id fact at all, not an empty one'
  fi
  # AND THE ROW SELECTION IS UNCHANGED: adding a string fact must not be able to move `find_job`
  # onto a different row. The review row here answers to the same id and carries no git_ref, so a
  # regression that let the new fact influence selection would surface as the wrong git_ref.
  _jm_run 4656 "$(printf '{"id":4656,"job_id":4656,"source_machine_id":"decoy","job":{"id":4656,"git_ref":"aaa..bbb","status":"done","model":"m"}}')" || true
  if [ "$(sed -n 's/^git_ref=//p' "$_jm_facts" | head -1)" = 'aaa..bbb' ]; then
    ok 'case (jm9) the git_ref-bearing row is still the one selected (the new fact cannot move find_job)'
  else
    bad 'case (jm9) adding source_machine_id changed which row find_job selects — the required facts now come from the wrong object'
  fi
fi

printf '== (jm10) #3654: --help documents the per-daemon scope, a WORKING check, and the prompt evidence ==\n'
CASE_N=$((CASE_N + 1))
OUT="$tmp/out-$CASE_N.txt"
bash "$WRAPPER_REAL" --help >"$OUT" 2>"$tmp/jm-help-err.txt"
if [ -s "$tmp/jm-help-err.txt" ]; then
  bad "case (jm10) --help wrote to stderr: $(head -2 "$tmp/jm-help-err.txt")"
else
  ok 'case (jm10) --help is clean on stderr'
fi
assert_says 'case (jm10) --help states that job ids are per-daemon' 'JOB IDS ARE PER-DAEMON, NOT GLOBAL'
assert_says 'case (jm10) --help says to verify git_ref, never the id alone' "VERIFY THE RECORD'S git_ref AND NEVER THE ID"
# THE PRESCRIBED COMMAND MUST BE ONE THAT WORKS. The issue and its addendum both prescribed
# `roborev show <id> --json | jq '{id, git_ref, branch, source_machine_id, token_usage}'`, which
# MEASURES four nulls: show nests those fields under `.job` and carries no source_machine_id at all.
# Documenting it unchanged would reproduce the very defect the addendum diagnoses.
assert_says 'case (jm10) --help reads git_ref from the payload that HAS it (.job on show)' \
  "roborev show <id> --json \| jq '\.job \| \{id, git_ref"
assert_says 'case (jm10) --help reads the daemon id from the LIST payload' \
  "roborev list --json --repo <abs-repo> --branch <branch>"
assert_lacks 'case (jm10) --help does NOT prescribe the top-level jq that measures nulls' \
  "show <id> --json \| jq '\{id, git_ref"
assert_says 'case (jm10) --help warns that the list default follows the --repo HEAD, not the shell' \
  'defaults its branch filter to the CURRENT HEAD OF THE --repo PATH'
assert_says 'case (jm10) --help states that a local row count proves nothing' \
  'A LOCAL ROW COUNT IS NOT EVIDENCE OF UNIQUENESS'
assert_says 'case (jm10) --help names the failure class the row count belongs to' \
  'IDENTICAL under the two states it claims'
assert_says 'case (jm10) --help names the prompt as the DIRECT ARTIFACT to lead with' \
  'That is the DIRECT ARTIFACT'
assert_says 'case (jm10) --help names the retrieval command' 'roborev show <id> --prompt'
# THE CORRECTED EVIDENCE AXIS (roborev job 39, finding 2). The first version called the prompt
# DIRECT EVIDENCE and token accounting a mere FALLBACK, which inverts the trust properties: the
# prompt is PARTLY BRANCH-INFLUENCED (roborev embeds repository-controlled content at positions
# indistinguishable from its own text, so a branch can mimic the delivery wording), while the token
# counts are recorded BY THE DAEMON and unwritable by the reviewed branch. Both halves are asserted
# — the qualification AND the corroboration property — because dropping either one restores a claim
# an authorizer would act on.
assert_says 'case (jm10) --help says the prompt is NOT self-authenticating' \
  'IT IS NOT SELF-AUTHENTICATING'
assert_says 'case (jm10) --help names the repository-controlled content that makes it so' \
  'EMBEDS repository-controlled content'
assert_says 'case (jm10) --help refuses "a human reads it" as a channel separation' \
  'not a channel separation'
assert_says 'case (jm10) --help gives token accounting its daemon-recorded corroboration property' \
  'recorded BY THE DAEMON and nothing'
assert_says 'case (jm10) --help ranks neither: they are complementary' \
  'COMPLEMENTARY rather than ranked'
assert_lacks 'case (jm10) --help no longer demotes token accounting to a fallback' \
  'the FALLBACK, for when the prompt cannot be retrieved'
# THE DECLARED CROSS-BOX BOUNDARY (roborev job 39, finding 1). The marker travels through GitHub
# while --recheck-job reads the LOCAL daemon, so a same-id/same-range collision across two boxes
# lets an authorization cross reviews. It is NOT closed here (that needs a marker field, which ask 4
# forbids) — so what must never regress is that the text DECLARES it rather than implying safety.
assert_says 'case (jm10) --help declares the cross-box boundary' 'DECLARED BOUNDARY'
assert_says 'case (jm10) --help says what is NOT claimed' 'NOT CLAIMED: that a marker cannot cross boxes'
assert_says 'case (jm10) --help names the channel the marker actually travels' \
  'through GITHUB, not through the daemon'
assert_says 'case (jm10) --help refuses the issue-body argument as irrelevant to the marker' \
  'true of the RECORD and'
assert_says 'case (jm10) --help records the escalation rather than deciding it' \
  'PENDING on #3654'
assert_says 'case (jm10) --help states the mitigation is operational, not mechanical' \
  'OPERATIONAL, NOT MECHANICAL|is informational and CANNOT enforce'
assert_says 'case (jm10) --help says why this is not the deleted classifier' \
  'RESURRECTS NOTHING OF THE DELETED DELIVERY CLASSIFIER'
assert_says 'case (jm10) --help records that the marker grammar is unchanged' \
  'THE MARKER GRAMMAR IS DELIBERATELY UNCHANGED'
# NOTE: `assert_no_marker_form` is deliberately NOT applied here. `--help` is the ONE sanctioned
# place the marker form is written out — the diagnostics refuse to print it and point here instead
# — so a run of it necessarily carries the stem.
reset_stub

printf '== (jm13) #3654 r2: the lookup is scoped to the JOB record'"'"'s branch, not the ambient one ==\n'
# THE DAEMON'S JOB LIST IS BRANCH-FILTERED, so the branch used to scope the supplementary read
# decides whether the key resolves at all. Scoping by the branch the INVOCATION is on answers about
# a DIFFERENT branch whenever the job was enqueued under another name — and then `job-machine:`
# reads NOT RECORDED for a record that HAS a source_machine_id. Fixtured by making the two diverge:
# the daemon holds jobs for the RECORD's branch only, and the fixture is checked out on another.
reset_stub
STUB_ANNOUNCE_SHA="$jm_head"
STUB_SOURCE_MACHINE_ID="$JM_UUID"
STUB_JOB_BRANCH='issue-earlier-lane'
STUB_LIST_BRANCH='issue-earlier-lane'
run_wrapper "$jm_work"
assert_verdict 'case (jm13)' PASS 0
assert_says 'case (jm13) the daemon is still named when the job'"'"'s branch is not the current one' \
  "^job-machine: $JM_UUID \(source_machine_id; job ids are per-daemon\)$"
reset_stub

printf '== (jm14) #3654 r2: --recheck-job of an OLDER job on another branch still names the daemon ==\n'
# THE MODE THE MITIGATION IS DOCUMENTED FOR, and the one most likely to miss: a waiver is applied
# by rechecking a job that may have been enqueued before a rename or a rebase. Before the fix this
# rendered NOT RECORDED and the documented "compare job-machine: between the request and the
# recheck" would silently not work in the only mode it exists for — a claim the implementation did
# not deliver, which is worse than claiming nothing.
reset_stub
STUB_ANNOUNCE_SHA="$jm_head"
STUB_SOURCE_MACHINE_ID="$JM_UUID"
STUB_JOB_BRANCH='issue-earlier-lane'
STUB_LIST_BRANCH='issue-earlier-lane'
STUB_VERDICT_FIELD='P'
STUB_RECORD_OUTPUT="$CLEAN_TEXT"
run_wrapper "$jm_work" --recheck-job 4656
assert_verdict 'case (jm14)' PASS 0
assert_says 'case (jm14) a recheck of an older, differently-branched job names its daemon' \
  "^job-machine: $JM_UUID \(source_machine_id; job ids are per-daemon\)$"
assert_says 'case (jm14) and it is still a recheck' '^MODE: recheck '
reset_stub

printf '== (jm17) #3654 r3: a target BEYOND the first 50 rows is still found (depth, not width) ==\n'
# ROUND 2 FIXED THE BRANCH AND LEFT THE DEPTH. With the branch right, a fixed `--limit 50` still
# drops the target the moment the branch holds more than 50 jobs — and the jobs that sit deep are
# the OLD ones, which are exactly the ones a `--recheck-job` names. So the documented cross-box
# mitigation failed for its own primary case. This case puts the target under 60 newer rows: it
# FAILS against a fixed `--limit 50` (the key reads NOT RECORDED) and passes once the lookup
# retrieves until found. Deliberately 60, not 5000 — the property is "deeper than the first page",
# and a case that needs a huge fixture invites being trimmed later.
reset_stub
STUB_ANNOUNCE_SHA="$jm_head"
STUB_SOURCE_MACHINE_ID="$JM_UUID"
STUB_JOB_BRANCH='issue-earlier-lane'
STUB_LIST_BRANCH='issue-earlier-lane'
STUB_LIST_DEPTH=60
run_wrapper "$jm_work"
assert_verdict 'case (jm17)' PASS 0
assert_says 'case (jm17) the daemon is named for a job below the first page of rows' \
  "^job-machine: $JM_UUID \(source_machine_id; job ids are per-daemon\)$"
assert_lacks 'case (jm17) depth never yields a filler row'"'"'s machine id' 'filler-not-the-target'
reset_stub

printf '== (jm18) #3654 r3: a genuinely absent job says so, naming the depth searched ==\n'
# THE COMPLEMENT, and the reason the loop needs an affirmative end-of-list signal: retrieving
# "until found" must still TERMINATE when the job is not there at all, and must say that it
# reached the end of the daemon's list rather than merely giving up at some number.
reset_stub
STUB_ANNOUNCE_SHA="$jm_head"
STUB_JOB_BRANCH='issue-earlier-lane'
STUB_LIST_BRANCH='issue-earlier-lane'
STUB_LIST_DEPTH=10
STUB_LIST_OMIT_TARGET=1
run_wrapper "$jm_work"
assert_says 'case (jm18) the miss names the depth reached and that the list ended' \
  'searched to a depth of [0-9]+ rows and to the end of what the daemon returns'
assert_lacks 'case (jm18) it never claims a fixed 50-job window' "latest 50 jobs"
reset_stub

printf '== (jm19) #3654 r3: an abort AFTER the record poll never emits a stale job-record state ==\n'
# THE PAIR OF KEYS MUST NOT CONTRADICT EACH OTHER. `job-machine:`'s UNAVAILABLE fallback names the
# `job-record:` state it inherits, and it used to be a fully-formed string built at INITIALIZATION,
# where `$JOB_RECORD` is `SKIP`. The `on_exit` EXIT trap emits the block on ANY abort, so a run that
# died after the record poll printed `job-record: PASS` beside
# `job-machine: UNAVAILABLE (... job-record: SKIP)` — an artifact stating two different things about
# one fact. Interpolating at that same early site does NOT fix it (the expansion still captures
# SKIP), which is why the value is built at EMIT time instead.
#
# Fixtured by injecting an abort between the poll and the recompute, into a SCRATCH COPY of the
# wrapper — the one place that window is reachable on demand.
_jmb_dir="$tmp/jm-abort"
mkdir -p "$_jmb_dir"
cp "$WRAPPER_REAL" "$SCRIPT_DIR/../flow/roborev-review-oracles.sh" \
  "$SCRIPT_DIR/../flow/roborev-review-checks.sh" "$SCAN_TOOL" "$_jmb_dir/"
if [ -f "$SCRIPT_DIR/../flow/roborev-job-facts.py" ]; then
  cp "$SCRIPT_DIR/../flow/roborev-job-facts.py" "$_jmb_dir/"
fi
if sed_inplace_verified "$_jmb_dir/roborev-review.sh" \
     's/^JOB_VERDICT=\$(fact verdict)$/exit 9\nJOB_VERDICT=$(fact verdict)/' \
     'exit 9' ; then
  reset_stub
  STUB_ANNOUNCE_SHA="$jm_head"
  run_wrapper --wrapper "$_jmb_dir/roborev-review.sh" "$jm_work"
  # The run aborted, so job-record: reached PASS while job-machine: never recomputed. The ONE
  # property: whatever job-record: says, job-machine: must say the SAME thing.
  _jmb_rec=$(sed -n 's/^job-record: //p' "$OUT" | head -1)
  _jmb_mach=$(sed -n 's/^job-machine: //p' "$OUT" | head -1)
  if [ -z "$_jmb_mach" ]; then
    bad "case (jm19) no job-machine: line was emitted on the abort path (job-record: '${_jmb_rec:-<none>}')"
  elif [ -z "$_jmb_rec" ]; then
    bad 'case (jm19) no job-record: line was emitted, so the two keys could not be compared'
  else
    case "$_jmb_mach" in
      *"job-record: $_jmb_rec"*)
        ok "case (jm19) job-machine: names the CURRENT job-record state ($_jmb_rec) on an abort" ;;
      *)
        bad "case (jm19) the two keys contradict each other: job-record: '$_jmb_rec' but job-machine: '$_jmb_mach'" ;;
    esac
  fi
  reset_stub
else
  bad 'case (jm19) could not inject the abort into the scratch wrapper, so the stale-state path was never exercised (a green here would be a probe failing in the direction that looks like success)'
fi

printf '== (jm15) #3654 r2: NO ambient-branch fallback when the record does not name its branch ==\n'
# THE FALLBACK THAT MUST NOT EXIST. Here the daemon WOULD answer for the fixture's own branch, so a
# retry against it would produce a uuid — one belonging to whatever that branch's job is, presented
# as this job's daemon. Silently answering about a different branch is the same defect one layer
# down, so the state must say what it could not do instead.
reset_stub
STUB_ANNOUNCE_SHA="$jm_head"
STUB_SOURCE_MACHINE_ID="$JM_UUID"
STUB_JOB_BRANCH='none'
STUB_LIST_BRANCH=$(git -C "$jm_work" rev-parse --abbrev-ref HEAD)
run_wrapper "$jm_work"
assert_verdict 'case (jm15) an unscopeable lookup never fails a clean run' PASS 0
assert_says 'case (jm15) the state names what could not be scoped' \
  '^job-machine: NOT RECORDED \(the job record does not name its own branch'
assert_says 'case (jm15) and says the ambient fallback was refused, with the reason' \
  'deliberately NOT retried against the branch this invocation is on, which would answer about a different branch'
assert_lacks 'case (jm15) no uuid is invented from the ambient branch' "^job-machine: $JM_UUID"
reset_stub

printf '== (jm16) #3654 r2: a record that WAS read but is incomplete is NOT RECORDED, not UNAVAILABLE ==\n'
# THE STATE THAT COULD LIE. The rendering used to branch on `record_required_present`, which asks
# about COMPLETENESS — so a record that was read and is merely nonterminal rendered
# 'UNAVAILABLE (no job record could be read)', which is FALSE: one was read. The three states are
# only worth having if each is an affirmative TRUE statement about what happened.
reset_stub
STUB_ANNOUNCE_SHA="$jm_head"
STUB_STATUS='running'
run_wrapper "$jm_work"
assert_verdict 'case (jm16)' FAIL 1
assert_says 'case (jm16) an incomplete-but-READ record is NOT RECORDED' '^job-machine: NOT RECORDED \('
assert_lacks 'case (jm16) it never claims no record could be read' '^job-machine: UNAVAILABLE'
assert_says 'case (jm16) the job-record key independently reports the incompleteness' '^job-record: DEGRADED'
reset_stub

printf '== (jm11) #3654: a `show` payload whose TOP-LEVEL id is NOT the asked job ==\n'
# MEASURED SHAPE (ten records): top-level `id` is the REVIEW row's own sequence and can differ from
# the job asked for — asking for 9 returns id=8, job_id=9, job.id=9. Two things must hold, and the
# second is what this change could have broken: the facts must still come from the JOB row (find_job
# matches id/job_id/job and then PREFERS the object carrying git_ref), and the OUTER row's own
# `source_machine_id` must NOT leak into the facts — a decoy machine id attributed to the wrong row
# is worse than none, because it reads as a measurement.
if [ ! -f "$_jm_tool" ] || ! command -v python3 >/dev/null 2>&1; then
  printf 'SKIP - roborev-job-facts.py or python3 unavailable; the id-divergence case did not run\n'
else
  _jm_run 9 "$(printf '{"id":8,"job_id":9,"uuid":"outer-uuid","source_machine_id":"DECOY","prompt":"p","job":{"id":9,"git_ref":"aaa..bbb","status":"done","model":"m"}}')" || true
  if [ "$(sed -n 's/^git_ref=//p' "$_jm_facts" | head -1)" = 'aaa..bbb' ]; then
    ok 'case (jm11) the nested job row is still selected when the top-level id names another review'
  else
    bad 'case (jm11) the id-divergent payload selected the wrong row — the required facts would come from the review row'
  fi
  if grep -q '^source_machine_id=DECOY$' "$_jm_facts"; then
    bad 'case (jm11) the OUTER review row supplied source_machine_id — the daemon id would be attributed to a row that is not the job'
  else
    ok 'case (jm11) no source_machine_id leaks from the outer review row (the fact follows the selected row)'
  fi
fi

printf '== (jm12) #3654 structural: the supplementary machine read NAMES THE BRANCH ==\n'
# `roborev list` is BRANCH-FILTERED, and its default follows the CURRENT HEAD OF THE `--repo` PATH
# — measured: from a cwd that is not a git repository at all, `--repo <lane>` returns that lane's
# branch's rows, and the same `--repo` run from a different lane's branch returns the same rows. So
# the default is `$BRANCH` (read from that same HEAD), which is the branch the CHECKOUT is on and
# NOT the job's. The two diverge on exactly the path this key is documented for: a `--recheck-job`
# of an older job, a renamed or rebased lane, or a checkout since moved on. A default-scoped or
# `$BRANCH`-scoped machine read then resolves NOTHING for a record that HAS a source_machine_id,
# and `job-machine:` reads NOT RECORDED for a reason having nothing to do with the record — a key
# that can only ever report absence. Only a structural assert pins this: every hermetic case runs
# with the fixture branch checked out, so the stub answers either way.
_jm_supp=$(sed -n '/^read_machine_fact() {/,/^}/p' "$WRAPPER_REAL")
if [ -z "$_jm_supp" ]; then
  bad 'case (jm12) could not locate read_machine_fact to inspect'
elif printf '%s\n' "$_jm_supp" | grep -qE 'roborev list --json .*--branch "\$job_branch"'; then
  ok 'case (jm12) the supplementary machine read is scoped to the RECORD'"'"'s own branch'
  # AND NEVER TO THE AMBIENT ONE. `$BRANCH` is the branch this invocation is running on, which is
  # not the job's fact; scoping by it answers about a different branch on exactly the recheck path
  # the operator mitigation is documented for. A fallback to it is as wrong as using it outright,
  # so the function must not mention it at all.
  if printf '%s\n' "$_jm_supp" | grep -q 'BRANCH'; then
    bad 'case (jm12) read_machine_fact still refers to the ambient $BRANCH — scoping (or falling back) to it answers about a branch other than the job own'
  else
    ok 'case (jm12) it never falls back to the ambient $BRANCH'
  fi
else
  bad 'case (jm12) the supplementary machine read is not scoped to the record'"'"'s own branch ($job_branch) — it would answer about whatever branch the invocation happens to be on'
fi
# AND THE RECORD READ IS DELIBERATELY LEFT ALONE: `sha-assert` depends on it, and naming the branch
# there is behaviour-neutral only while the invoking cwd's branch equals $BRANCH. Pinned so a future
# "consistency" edit has to argue with this line rather than silently change a load-bearing read.
if grep -qE '^ *list\) json=\$\(roborev list --json --limit 50 --repo "\$REPO" 2>/dev/null' "$WRAPPER_REAL"; then
  ok 'case (jm12) the RECORD read is unchanged (no --branch) — that read is sha-assert'"'"'s, and out of scope here'
else
  bad 'case (jm12) the job-record list read has changed shape — sha-assert depends on it, and #3654 must not have touched it'
fi

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
# ===== STDERR IS CAPTURED SEPARATELY, AND MUST BE EMPTY (#3392) =====
# `usage()` is an UNQUOTED heredoc (`cat <<EOF`) so that $PROGNAME interpolates, which makes a
# BACKTICK in its body COMMAND SUBSTITUTION. A markdown-style `--base` in the help text therefore
# (a) printed `--base: command not found` on stderr and (b) SILENTLY DELETED the backticked term
# from the rendered help. (b) is the damage: the waiver passage is the ONLY sanctioned place an
# operator learns WHICH sha to copy into a marker — the diagnostics deliberately refuse to print
# the marker and point here instead — so a corrupted help produces a waiver naming the wrong base,
# which then reports STALE. This class is INVISIBLE to a source read and to a diff review; only
# EXECUTING `--help` and looking at stderr finds it.
# The two streams are kept apart on purpose: merged into $OUT, the error lines would have satisfied
# nothing and failed nothing, which is exactly how this shipped.
HELP_ERR="$tmp/help-stderr-$CASE_N.txt"
STUB_INVOKED="$INVOKED" PATH="$stubbin:$PATH" bash "$WRAPPER_REAL" --help >"$OUT" 2>"$HELP_ERR"
RC=$?
if [ "$RC" -eq 0 ]; then ok '--help exits 0'; else bad "--help exited $RC (want 0)"; fi
if [ -s "$HELP_ERR" ]; then
  bad "--help wrote to STDERR, so the usage heredoc is being EXECUTED, not printed — a backtick (command substitution in an unquoted heredoc) also DELETES the backticked term from the rendered text: $(tr '\n' ' ' <"$HELP_ERR")"
else
  ok '--help writes NOTHING to stderr — no part of the usage heredoc is executed'
fi
# The heredoc must stay backtick-free, which is the pre-existing convention in this body: the
# delimiter cannot be quoted (the body interpolates $PROGNAME and friends), so there is no escaping
# strategy to get right — only the absence of backticks.
_help_body=$(awk '/^usage\(\) \{/ { inu = 1 } inu { print } inu && /^EOF$/ { exit }' "$WRAPPER_REAL")
case "$_help_body" in
  '') bad '--help: the usage heredoc could not be extracted, so its backtick-freedom was not checked' ;;
  *'`'*) bad '--help: the usage heredoc contains a BACKTICK. The delimiter is unquoted (so $PROGNAME interpolates), which makes a backtick command substitution: it prints an error to stderr AND deletes the term from the rendered help. Use plain text — the surrounding help prose names flags and keys unquoted.' ;;
  *) ok '--help: the usage heredoc is backtick-free, so nothing in it can be executed or silently deleted' ;;
esac
# THE SEMANTIC HALF, because an empty stderr is not sufficient: the terms an operator has to COPY
# must actually be PRESENT in the rendered text. A future backtick around a different term would be
# caught by the two asserts above, but so would a well-meaning rewrite that simply dropped them.
for _hterm in 'base=' '--base' 'assert-base:'; do
  if grep -qF -- "$_hterm" "$OUT"; then
    ok "--help renders the term an operator must copy: $_hterm"
  else
    bad "--help does not render '$_hterm' — the waiver passage cannot tell a human which sha to name, and this is what a swallowed backtick looks like"
  fi
done
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
# #3229: the RETAINED facts about how roborev drops paths. The mechanism statement stays —
# it is what stops the falsified "roborev filters non-code" claim coming back — while the
# key that once PREDICTED the effective exclusion set is gone (#3283).
assert_says '--help states the CORRECTED mechanism, not the falsified one' 'roborev drops exactly what its'
assert_lacks '--help never restates the falsified claim' '[Ee]xcludes non-code paths'
assert_says '--help defines docs-only as a code-free CENSUS, not a path prefix' 'CENSUS as code-free: classifies it, NEVER a .docs/. path prefix'
assert_says '--help names the harness convention as reviewed code' 'docs/reports/\*-artifacts/ are executable code'
# THE REMOVAL, ASSERTED IN THE DOCUMENTED OUTPUT CONTRACT (#3229 owner ruling / #3283). A
# `--help` that still documents a key the wrapper cannot emit is a claim of coverage that does
# not exist — the failure direction that reads as coverage — so every trace of the deleted
# oracle's grammar must be absent, not merely unexercised.
assert_lacks '--help no longer documents the census-exclusion key' 'census-exclusion'
assert_lacks '--help no longer names the ported Go function' 'FormatExcludeArgs'
assert_lacks '--help no longer documents the swallowed-paths FAIL grammar' 'code census paths excluded'
assert_lacks '--help no longer documents the trailing-slash FAIL' 'trailing-slash pattern'
assert_lacks '--help no longer documents the drift FAIL' 'exclusion set drift'
assert_lacks '--help no longer documents the UNCORROBORATED FAIL' 'UNCORROBORATED'
assert_lacks '--help no longer enumerates config sources it does not read' 'UNION OF THREE CONFIG FILES'
assert_lacks '--help no longer documents a root-config source tag' '\[root-config\]'
assert_lacks '--help no longer documents the built-in-set key' 'built-in-set:'
assert_lacks '--help no longer documents a roborev-builtin source tag' '\[roborev-builtin\]'
# The block's KEY CONTRACT is what readers grep, so --help must list exactly the keys the
# wrapper emits.
assert_says '--help still documents the prompt-content key' 'prompt-content'
# #3312 (owner ruling (4)): the CONTRACT --help documents is now the ONE QUESTION plus the waiver.
# An operator reading an absence FAIL must be able to learn from --help what was asked, that the
# machine cannot tell WHY the paths are absent, and exactly how a human may waive it — including the
# limitation that authorship is not mechanically verified. Each of these is a promise to a reader, so
# each is asserted rather than assumed.
assert_says '--help documents the single question, with no delivery classifier' \
  'ONE QUESTION, NO DELIVERY CLASSIFIER'
assert_says '--help says absence is unconditionally a FAIL' \
  'ABSENT is a FAIL, unconditionally, whatever caused it'
assert_says '--help states the accepted cost: the two absences are indistinguishable' \
  'IDENTICAL to the machine'
assert_says '--help names the token accounting as the human evidence' '398k-649k input'
assert_says '--help gives the waiver marker verbatim, with every bound field' \
  'roborev-waive: prompt-content-absent base=<40-hex> head=<40-hex> job=<id> reason=<why>'
assert_says '--help says who may grant it, and that a worker may only request' \
  'may REQUEST one'
assert_says '--help says the waiver is bound to the whole review scope' \
  'IT IS BOUND TO THE WHOLE REVIEW SCOPE'
assert_says '--help says a re-run needs a fresh waiver' 'a push, a different base or a$|re-run each need a fresh one'
assert_says '--help states the three anti-self-grant layers' \
  'THREE THINGS STOP THE DOCUMENTATION BECOMING THE CREDENTIAL'
assert_says '--help says the diagnostic prints no part of the marker' \
  'diagnostic prints NO part of the$|marker; it points here instead'
assert_says '--help says it excuses the absence verdict only' 'excuses the ABSENCE'
assert_says '--help says the waived token is DISTINCT from PASS' 'a DISTINCT'
assert_says '--help states the authorship limitation, not an implied guarantee' \
  'PROCESS-ENFORCED WITH AN AUDIT TRAIL, NOT MECHANICALLY$|VERIFIED'
assert_says '--help says the author must be on an explicit allowlist' \
  'THE AUTHOR MUST BE ON AN EXPLICIT ALLOWLIST'
assert_says '--help scopes the residual to which allowlisted human' 'WHICH ALLOWLISTED HUMAN'
assert_lacks '--help no longer documents the deleted snapshot record keys' 'snapshot-path/-containment/-expected'
assert_lacks '--help no longer claims two delivery modes are treated differently' 'TWO DIFF-DELIVERY MODES'
assert_never_enqueued '--help'

printf '== structural: path normalisation has EXACTLY ONE boundary ==\n'
# THE INVARIANT THAT STOPS THE NEXT ROUND (#3229 round 4). Rounds 2, 3 and 4 produced six
# blockers and every one was a path-normalisation defect in a DIFFERENT consumer, because
# normalisation was scattered: the census did not normalise at all, a since-deleted
# consumer unquoted at one point, `prompt-content:` did something else again. Patching the reported
# consumer each round is a losing game, so the boundary itself is asserted here:
#   (1) every git path read is `-z`, so paths arrive RAW and there is nothing to unquote;
#   (2) RAW is the single internal representation — no consumer unquotes a census path;
#   (3) there is ONE unquoting implementation and ONE header matcher, with the unquoter
#       called only from the matcher;
#   (4) no consumer re-implements header parsing or newline-delimited path membership.
ORACLES="$SCRIPT_DIR/../flow/roborev-review-oracles.sh"
CHECKS_FILE="$SCRIPT_DIR/../flow/roborev-review-checks.sh"
FLOW_FILES=("$ORACLES" "$CHECKS_FILE" "$WRAPPER_REAL")
for _f in "${FLOW_FILES[@]}"; do
  if [ ! -f "$_f" ]; then bad "structural: missing $_f"; continue; fi
  # (1) EVERY `git diff` that reads PATHS must be NUL-delimited. A `--numstat`/`--name-only`
  #     read without `-z` C-quotes odd paths and re-creates the whole defect class.
  _bad_reads=$(grep -nE 'git .*diff .*(--numstat|--name-only)' "$_f" \
    | grep -v '^[0-9]*: *#' | grep -vF -- ' -z ' || true)
  if [ -z "$_bad_reads" ]; then
    ok "structural: every path-reading git diff in $(basename "$_f") is NUL-delimited (-z)"
  else
    bad "structural: a path-reading git diff without -z in $(basename "$_f"): ${_bad_reads%%$'\n'*}"
  fi
done
# (2) + (3): the unquoter is DEFINED once and CALLED only from the canonical matcher.
_unq_defs=$(grep -cE '^roborev_unquote_path\(\) \{' "$ORACLES" || true)
if [ "${_unq_defs:-0}" -eq 1 ]; then
  ok 'structural: roborev_unquote_path is defined exactly once, in the oracles file'
else
  bad "structural: expected exactly 1 definition of roborev_unquote_path, found ${_unq_defs:-0}"
fi
_unq_callers=$(grep -nE '(^|[^#])roborev_unquote_path ' "$ORACLES" "$CHECKS_FILE" "$WRAPPER_REAL" \
  | grep -v '^[^:]*:[0-9]*: *#' || true)
_unq_caller_files=$(printf '%s\n' "$_unq_callers" | sed -n 's|^\([^:]*\):.*|\1|p' | sort -u | wc -l | tr -d '[:space:]')
if [ "${_unq_caller_files:-0}" -eq 1 ] && printf '%s' "$_unq_callers" | grep -qF 'roborev-review-oracles.sh'; then
  ok 'structural: roborev_unquote_path is called ONLY from the oracles file (one boundary)'
else
  bad "structural: roborev_unquote_path is called from $_unq_caller_files file(s) — a second consumer normalises on its own"
fi
# Every call site must sit inside `roborev_diff_header_has_path`, the one place text we did
# NOT get from git plumbing is normalised. Bounded by the next top-level function.
_matcher_start=$(grep -nE '^roborev_diff_header_has_path\(\) \{' "$ORACLES" | head -1 | cut -d: -f1)
if [ -z "$_matcher_start" ]; then
  bad 'structural: roborev_diff_header_has_path is not defined — the canonical matcher is gone'
else
  _matcher_end=$(awk -v s="$_matcher_start" 'NR>s && /^}/ {print NR; exit}' "$ORACLES")
  _outside=0
  while IFS= read -r _line; do
    [ -n "$_line" ] || continue
    _n="${_line#*:}"; _n="${_n%%:*}"
    case "$_line" in "$ORACLES":*) ;; *) _outside=$((_outside + 1)); continue ;; esac
    if [ "$_n" -lt "$_matcher_start" ] || [ "$_n" -gt "${_matcher_end:-$_matcher_start}" ]; then
      _outside=$((_outside + 1))
    fi
  done <<<"$_unq_callers"
  if [ "$_outside" -eq 0 ]; then
    ok "structural: every roborev_unquote_path call is inside roborev_diff_header_has_path (lines $_matcher_start-${_matcher_end:-?})"
  else
    bad "structural: $_outside roborev_unquote_path call(s) outside the canonical matcher"
  fi
fi
# (4) no consumer re-implements header parsing or newline-delimited membership. These are
#     the exact three mechanisms that were wrong: a `[^ ]+` header regex, a `.promptpaths`
#     path-set file, and `grep -Fxq` membership over newline-delimited paths.
# COMMENT LINES ARE EXEMPT: the file DOCUMENTS what was retired and why, which is the
# record that keeps a future edit from reintroducing it. Only executable lines are checked.
for _pat in 'diff --git a/\[\^ \]' 'promptpaths' 'grep -Fxq'; do
  if grep -nE -- "$_pat" "$CHECKS_FILE" 2>/dev/null | grep -qv '^[0-9]*: *#'; then
    bad "structural: roborev-review-checks.sh still EXECUTES the retired mechanism '$_pat'"
  else
    ok "structural: the retired mechanism '$_pat' is not executed by roborev-review-checks.sh"
  fi
done
if grep -qE '^ *if roborev_diff_header_has_path ' "$CHECKS_FILE"; then
  ok 'structural: prompt-content decides membership through the canonical matcher, per header'
else
  bad 'structural: prompt-content does not call roborev_diff_header_has_path — it has its own matcher again'
fi
# (5) HEADER COLLECTION lives with the matcher too (#3229 round 5, blocker 1). Resolving the
#     header ambiguity needs the `rename from`/`rename to` lines that FOLLOW the header, so
#     "which lines belong to a header" is header-shape knowledge — and this file must not
#     grow a second, subtly different idea of the extended-header run. It therefore does no
#     `diff --git` scanning of its own at all. Only a SCAN counts — the key's ERROR prose
#     legitimately says the words "diff --git" when it explains what was looked for.
if grep -nE '(grep|awk|sed|case)[^#]*diff --git' "$CHECKS_FILE" 2>/dev/null | grep -qv '^[0-9]*: *#'; then
  bad 'structural: roborev-review-checks.sh EXECUTES its own diff --git scan — header-shape knowledge must stay with the matcher in the oracles file'
else
  ok 'structural: roborev-review-checks.sh does no diff --git scanning of its own'
fi
_coll_defs=$(grep -cE '^roborev_collect_prompt_headers\(\) \{' "$ORACLES" || true)
if [ "${_coll_defs:-0}" -eq 1 ] && grep -qE '^ *roborev_collect_prompt_headers "\$PROMPT_FILE"' "$CHECKS_FILE"; then
  ok 'structural: the prompt headers (and their rename from/to lines) are collected by the oracles file'
else
  bad "structural: roborev_collect_prompt_headers is not the single collector (defs in oracles: ${_coll_defs:-0}) or prompt-content does not call it (#3312 ruling (4): the prompt is the only source there is)"
fi
# (6) C⁗ (#3312): WHAT MUST STILL BE TRUE NOW THAT NOTHING IS READ. The observer, the digest, the size, the
#     bounded-read watchdog and the three-valued path-state helper are all DELETED by owner ruling, so the
#     asserts that pinned them are deleted with them and the ones below take their place.
#
#     THE PREDICATE-FAMILY LINT GOES WITH ITS SUBJECT, deliberately: it scanned the capture apparatus for bare
#     file predicates, and that apparatus performs no filesystem access any more — a lint with an EMPTY subject
#     set greens vacuously, which is the very shape it existed to catch. The durable artifact is the RULE, kept
#     in CLAUDE.md and the doctrine page: every `test`/`[` file predicate is two-valued, so it must collapse
#     "cannot tell" onto one of its answers, and it always picks the permissive one. The "performs NO
#     filesystem access" assert below is what makes that empty subject set TRUE rather than assumed.
# ONE DEFINITION OF THE EXECUTABLE-LINE FILTER (#3367). The classifier scan, its control and the
# order-independence pin below all need the same "strip comments and the --help heredoc" pass. It
# used to be written out three times, and a port is a second implementation whose correctness is
# only knowable by differential testing against the original — the exact trap CLAUDE.md records for
# #3283. So it is a function with ONE body: a filter that drifts between call sites cannot make one
# caller lenient while its own control still reads green.
# THE SAME FILTER WITHOUT THE HEREDOC EXEMPTION. Only the four PROSE-SHAPED retired tokens
# (mixed-delivery, delegated-oversize, snapshot-unbound, unparseable-instruction) can legitimately
# appear in help text, so only they need the exemption. The thirteen CODE IDENTIFIERS never can --
# they are snake/SCREAMING case, not English -- so they are scanned EVERYWHERE, heredoc included.
#
# That split is what retires the whole "is this heredoc line executable?" problem. Rounds 19-21 of
# review each found another construct that executes inside an unquoted heredoc -- a command
# substitution, an assigning parameter expansion, a bare `$VAR`, a substitution spanning lines --
# and each fix was another claim about a SET. With identifiers never exempt, none of it matters:
# a reintroduction is caught wherever it is written, and prose naming a retired STATE stays legal
# exactly where prose belongs. Measured on the real wrapper: 0 identifiers in the heredoc, 1 prose
# word, so this costs nothing and closes the family. (Issue comments 3/5/6/7 recommended keying on
# the identifiers for this reason; deferred then as scope, adopted now because it fixes a live hole.)
_cls_all_lines() {
  # NO FILTERING AT ALL -- not even comments. A `#`-leading line inside the UNQUOTED help heredoc is
  # DATA, not a comment, and its expansions still execute, so skipping such lines was a hole
  # (roborev job 56). Deciding which `#` lines are comments needs heredoc state, i.e. the parsing
  # problem this split exists to avoid. Measured instead: the three flow scripts contain ZERO
  # mentions of any of the thirteen identifiers in comment lines, so scanning everything costs
  # nothing. If that ever stops being true this check reds, and the right response is to rename the
  # identifier or drop it from the list -- NOT to reintroduce a filter.
  cat "$@" 2>/dev/null || true
}
# ===== THE CLASSIFIER IS GONE, AND MUST STAY GONE (owner ruling (4), #3312) =====
# Every state, helper and marker below carried one of the four High-severity false verdicts. They are
# asserted ABSENT rather than fixed, because absence is what the ruling bought: with no delivery-mode
# inference there is nothing left for a fifth round to find wrong. A reintroduction is a design
# regression, not a refactor, and it reds here.
# EXECUTABLE LINES ONLY, AND THE FILTER IS THE WHOLE DIFFICULTY (#3392). A comment that RECORDS what
# was deleted — and why it may not come back — is the durable artifact of this ruling, so scanning
# prose would make the history itself a violation. `--help` PROSE is prose for exactly the same
# reason and was NOT being exempted: the usage heredoc names the retired states while telling the
# reader they are gone, which this check would read as a reintroduction.
#
# AND THE OLD PREDICATE WAS FAIL-OPEN, NON-DETERMINISTICALLY (found here, #3392). It was
# `grep -hv ... | grep -qF "$needle"`, and under this file's `pipefail` the DOWNSTREAM `grep -q`
# exits at its FIRST match, which SIGPIPEs the upstream grep, so the PIPELINE status became 141 —
# non-zero — and a real violation was recorded as CLEAN. Whether it did so depended on whether the
# upstream had finished writing, i.e. on the BYTE OFFSET of the match: adding an unrelated comment
# block to the wrapper flipped this very check from clean to violated with no change to its subject.
# A guard whose verdict depends on a pipe buffer is worse than no guard. So the text is captured ONCE
# and matched in PURE BASH with `case` — no pipe, no subshell, no exit status to lose.
_classifier=""

# THE 13 CODE IDENTIFIERS: scanned EVERYWHERE, including the help heredoc. They cannot occur in
# English, so there is no legitimate prose use to exempt.
_cls_ident_list='roborev_collect_review_diff_headers
roborev_prompt_snapshot_paths
roborev_snapshot_path_binding
ROBOREV_DIFF_SOURCE_STATE
BLOCKRESET
BLOCKHDR
in_trailer
in_fence
_rx_delivery_hdrs
_rx_snap_paths
SNAPSHOT_NOTICE
ROBOREV_SNAPSHOT_PATH
ROBOREV_SNAPSHOT_CONTAINMENT'
# THE ONE CLASSIFIER PREDICATE (roborev job 61). The real scan, AC2, AC3 and AC3b all call this, so
# no caller can supply its own answer. They used to: AC2 carried a literal no-op
# (`case ... in *"$tok"*) : ;; esac`) and AC3 hard-coded two token names, which meant AC3's stated
# criterion -- "observed firing" -- was satisfied by a control that could not fail. An AC verified by
# a control that supplies its own answer is not verified, and this is the SECOND instance of
# "control certifies a copy, not the enforcer" in this PR; the first had already DRIFTED by the time
# it was found. One helper makes a third instance unexpressible rather than merely unlikely.
_cls_hits() {
  for _ch_tok in $_cls_ident_list; do
    case "$1" in *"$_ch_tok"*) printf '%s\n' "$_ch_tok" ;; esac
  done
}
_cls_all=$(_cls_all_lines "$ORACLES" "$CHECKS_FILE" "$WRAPPER_REAL")
if [ -z "$_cls_all" ]; then
  bad 'structural: the all-lines capture for the classifier scan is EMPTY, so the identifier scan below would report CLEAN having examined nothing'
fi
_classifier=$(_cls_hits "$_cls_all" | tr '\n' ' ')
# THE 4 PROSE-SHAPED VERDICT STRINGS, IN AN ASSIGNMENT CONTEXT ONLY (roborev job 63). Dropping them
# outright closed six rounds of heredoc parsing but opened a real AC3 gap: a classifier rebuilt under
# NEW variable names still emits the OLD verdict strings, so `delivery_mode=mixed-delivery` read as
# GONE. A verdict string in PROSE is not a classifier; a verdict string being ASSIGNED is.
#
# This is issue comment 3's option (b) -- "require the token to appear in an assignment context
# rather than as a bare substring" -- which that comment ranked first and I passed over for option
# (a). It needs NO heredoc exemption, because an assignment does not occur in help text, so it does
# not reopen the parsing family. Measured on the three real flow scripts: 0 hits, and it matches
# both `delivery_mode=mixed-delivery` and `mode="mixed-delivery"` while leaving the usage prose alone.
_cls_prose_assigned=$(printf '%s\n' "$_cls_all" \
  | grep -oE '=["'"'"']?(mixed-delivery|delegated-oversize|snapshot-unbound|unparseable-instruction)' \
  | sed 's/^=["'"'"']*//' | sort -u | tr '\n' ' ')
_cls_prose_assigned=${_cls_prose_assigned% }
[ -z "$_cls_prose_assigned" ] || _classifier="$_classifier $_cls_prose_assigned"
_classifier=${_classifier# }
_classifier=${_classifier% }
# THE 4 PROSE-SHAPED VERDICT STRINGS ARE NOT SCANNED AT ALL (roborev jobs 51/53/54/56/57/58).
# `mixed-delivery`, `delegated-oversize`, `snapshot-unbound` and `unparseable-instruction` are
# English, so they legitimately appear in help text and doctrine -- which forced an exemption for
# the `usage()` heredoc, and SIX consecutive review rounds each found another construct that
# executes inside an unquoted heredoc (command substitution, assigning parameter expansion, bare
# $VAR, a substitution spanning lines, a `#` line that is data not a comment). Every fix was another
# claim about a SET, and the exemption is the only reason any of it was needed.
#
# Dropping them removes the exemption, the filter and the whole family. Nothing is lost that the
# guard could actually rely on: a reintroduced classifier is CODE, and code is these thirteen
# identifiers -- a verdict STRING alone is not a classifier. This is what issue comments 3/5/6/7
# recommended from the start ("key the guard solely on the 13 identifiers"); I deferred it as scope
# in 3367-W1 and six rounds of heredoc parsing were the price of that deferral.
if [ -z "$_classifier" ]; then
  ok 'structural: the delivery-mode classifier is GONE — no block/heading/fence/candidate state, no snapshot or delegated distinction, no NOTICE exemption'
else
  bad "structural: delivery-mode classification is back in the flow scripts —$_classifier. Owner ruling (4) deleted it because FOUR consecutive review rounds each found a High-severity false verdict in inferring structure from prompt text that embeds repository-controlled content (#3312). THIS IS NOT A FLAKE AND MUST NOT BE WAIVED: since #3367 the scan reads an immutable path, so this FAIL is deterministic and reproduces standalone (bash scripts/tests/test_roborev_review_guard.sh) — it is NOT the intermittent tooling-tests red of #2790/#2596/#2188"
fi
# THE CONTROL FOR THAT FILTER (#3392). The scan above reads the REAL files, so on a clean tree it is
# indistinguishable from a scan that examines nothing — which is what it had degraded into. The same
# awk filter is therefore run over a synthetic pair of files that carry the SAME forbidden token in
# both positions, and it must keep the executable one and drop the `--help` one. Without this, the
# prose exemption could widen until it swallowed the subject and the check would still read green.
# ===== BOTH DIRECTIONS, PINNED AGAINST THE REAL WRAPPER (#3367 AC2/AC3) =====
# The control above builds its own pair of files, which shows the FILTER discriminates but says
# nothing about the state of the actual subject. These two pin the real file, in both directions.
#
# (a) THE REAL WRAPPER'S DOCTRINE PROSE MUST NOT TRIP THE SCAN — and this pin is only worth
#     anything if that prose is actually THERE. The exemption is load-bearing precisely because
#     `roborev-review.sh` names the retired states in its `usage()` heredoc while telling the
#     reader they are gone (that occurrence is what red this component on two unrelated lanes).
#     If the wrapper ever stopped naming them, the pin would pass having tested nothing, so the
#     RAW presence is asserted FIRST and its absence is a FAIL, not a silent skip.
_ac2_prose=""
for _ac2_tok in mixed-delivery delegated-oversize snapshot-unbound unparseable-instruction; do
  grep -qF "$_ac2_tok" "$WRAPPER_REAL" && _ac2_prose="$_ac2_prose $_ac2_tok"
done
if [ -z "$_ac2_prose" ]; then
  bad 'structural (#3367 AC2): the real wrapper names NONE of the prose-shaped retired states, so the prose exemption is untested by this run — the guard would read green whether or not it works (the wrapper is supposed to record what was deleted; if that record is gone, restore it or retire this pin deliberately)'
else
  # The scan cannot trip on these because it does not look for them: the token list is the thirteen
  # CODE IDENTIFIERS only. Asserted against the list itself rather than against a filter's output,
  # so this stays true without a heredoc exemption existing at all.
  # Asked of the PREDICATE the real scan uses, over the REAL text -- not of the token list, and not
  # by a match this control performs itself. If the predicate ever starts reporting a prose word,
  # this fails; if the predicate stops reporting anything at all, AC3 below fails. Between them the
  # two directions pin the same function.
  _ac2_hits=$(_cls_hits "$_cls_all")
  _ac2_leaked=""
  for _ac2_tok in $_ac2_prose; do
    printf '%s\n' "$_ac2_hits" | grep -qxF "$_ac2_tok" && _ac2_leaked="$_ac2_leaked $_ac2_tok"
  done
  if [ -z "$_ac2_leaked" ]; then
    ok "structural (#3367 AC2): the real wrapper's doctrine prose names$_ac2_prose and NONE of them is in the scanned token list — recording what was deleted cannot be a violation, with no heredoc exemption needed to make that true"
  else
    bad "structural (#3367 AC2): a prose-shaped retired state survived the executable-line filter —$_ac2_leaked. TWO causes, and they need opposite responses: (a) the heredoc/comment exemption broke, which is the deterministic red that made two lanes investigate a diff touching no scripts/ path — fix the filter; or (b) that word now sits on a heredoc line carrying an EXPANSION, which is correctly NOT exempt because an unquoted heredoc executes it — move the word to an expansion-free line. Check which before touching the filter"
  fi
fi
# (b) A GENUINE REINTRODUCTION INTO THE REAL WRAPPER MUST STILL BE CAUGHT. Asserted against a COPY
#     of the real trio with one executable line appended — not the 2-line synthetic above — so the
#     exemptions cannot have widened far enough to swallow live code in the real file's own shape.
#     Without this, (a) could be satisfied by an exemption that drops everything.
_ac3_dir="$tmp/real-wrapper-reintroduction"
mkdir -p "$_ac3_dir"
cp "$WRAPPER_REAL" "$_ac3_dir/roborev-review.sh"
printf '\nROBOREV_DIFF_SOURCE_STATE="mixed-delivery"   # a genuine reintroduction\n' \
  >>"$_ac3_dir/roborev-review.sh"
_ac3_exec=$(_cls_all_lines "$ORACLES" "$CHECKS_FILE" "$_ac3_dir/roborev-review.sh")
_ac3_caught=$(_cls_hits "$_ac3_exec" | tr '\n' ' ')
# AC3b: THE SAME REINTRODUCTION, PARKED INSIDE usage() BEFORE ITS HEREDOC (roborev job 47). AC3
# above appends OUTSIDE the function, which cannot see a filter that exempts from `usage() {`
# rather than from `cat <<EOF` — a reintroduction between the two was invisible. An executable
# statement there is code like any other; only the heredoc BODY is prose.
_ac3b_dir="$tmp/real-wrapper-usage-reintroduction"
mkdir -p "$_ac3b_dir"
awk '/^usage\(\) \{/ && !done { print; print "  ROBOREV_DIFF_SOURCE_STATE=inline"; done = 1; next } { print }' \
  "$WRAPPER_REAL" >"$_ac3b_dir/roborev-review.sh"
_ac3b_exec=$(_cls_all_lines "$ORACLES" "$CHECKS_FILE" "$_ac3b_dir/roborev-review.sh")
_ac3b_caught=$(_cls_hits "$_ac3b_exec" | tr '\n' ' ')
case "$_ac3b_caught" in
  *ROBOREV_DIFF_SOURCE_STATE*)
    ok 'structural (#3367 AC3b): a reintroduction parked INSIDE usage() before its heredoc is caught — identifiers are scanned everywhere, so no part of the function is exempt' ;;
  *)
    bad 'structural (#3367 AC3b): a reintroduction between `usage() {` and its `cat <<EOF` was NOT caught. Since the identifier/prose split there is no heredoc exemption left to blame - identifiers are scanned on every line - so look at the _cls_hits predicate and the token list, not at a filter' ;;
esac
case "$_ac3_caught" in
  *ROBOREV_DIFF_SOURCE_STATE*)
    ok "structural (#3367 AC3): an executable classifier reintroduction appended to a COPY of the real wrapper is still caught —$_ac3_caught — so the prose exemptions narrow the scan without blinding it" ;;
  *)
    bad 'structural (#3367 AC3): an executable classifier reintroduction appended to a copy of the REAL wrapper was NOT caught. the classifier-GONE assert above is green by blindness. There is no prose exemption left to widen - check _cls_hits and the token list, which is exactly what this control exists to detect' ;;
esac
# THE THREE SNAPSHOT KEYS GO WITH IT: a block that still emitted them would be describing a
# measurement the wrapper no longer makes.
_skeys=""
for _sk in snapshot-path snapshot-containment snapshot-expected; do
  grep -qF "emit_kv '$_sk'" "$WRAPPER_REAL" && _skeys="$_skeys $_sk"
done
if [ -z "$_skeys" ]; then
  ok 'structural: the block emits no snapshot-* keys (nothing is classified, so there is nothing to record about a mode)'
else
  bad "structural: the block still emits —$_skeys. Those keys described the retired classifier's output (#3312 ruling (4))"
fi
# ===== THERE IS NO MUTABLE WRAPPER PATH (#3367) =====
# The order dependence this issue reports had one cause: `WRAPPER` was a GLOBAL that two gate-mock
# cases repointed at a scratch copy ~2000 lines from the doctrine asserts, so a scan reading it was
# judged against whichever file the last case had left behind.
#
# THIS WAS FIRST FIXED BY GUARDING THE GLOBAL, AND THAT APPROACH WAS RETIRED AS UNSOUND. A ~250-line
# lexical scanner classified every occurrence of `$WRAPPER` (quote state, arity, spelling, command
# position, markers) so a doctrine scan could not read the mutable one. Eighteen roborev rounds found
# forty-three defects in it, all valid, and the last one settled the question: `sh -c` executes a
# name assembled from adjacent quoted strings (`export WRAP''PER; sh -c 'grep "$WRAP''PER"'`), and
# the harness already uses `sh -c`. Banning `eval` did not close it, and the set does not close --
# `bash -c`, `source`, `.`, `xargs sh -c`. A LEXICAL SCANNER CANNOT DECIDE WHETHER A SHELL LINE READS
# A VARIABLE, BECAUSE THE NAME NEED NOT APPEAR IN THE LINE. No further rounds could have fixed that.
#
# So the global is GONE instead of guarded. `run_wrapper` -- the single site that ever invoked the
# wrapper on behalf of a case -- resolves its own path, and the two mock cases scope
# `RUN_WRAPPER_PATH` around themselves rather than mutating anything the rest of the file can see.
# Nothing is left to read wrongly, so there is nothing to police: one shell-enforced fact replaces
# the scanner, its thirty-six fixtures and its poison probe. The property below is what remains.
_wr_bad=""
# Ask the shell whether the global can exist, rather than guessing which spelling would create it.
if ( WRAPPER=/nonexistent/decoy ) 2>/dev/null; then
  _wr_bad="$_wr_bad a-mutable-WRAPPER-global-can-be-created;"
fi
if [ -n "${WRAPPER+set}" ]; then
  _wr_bad="$_wr_bad WRAPPER-has-a-value;"
fi
# THE SCRATCH-COPY PATH IS A `local`, SO THERE IS NO SECOND GLOBAL. `RUN_WRAPPER_PATH` was the
# first attempt: a global the two mock cases set and cleared. Guarding it meant counting its
# assignments, which matched only `NAME=` and so missed `export`/`declare`/`printf -v`/`read` --
# the third time this PR made the assignment-syntax enumeration error. A `local` cannot be assigned
# from outside its function, so the class is unexpressible rather than policed. Asked of the shell,
# not of the text: run_wrapper is invoked in a subshell that tries to leak the name outward.
if _wr_leak=$( { run_wrapper_probe() { local _rw_wrapper=inner; }; run_wrapper_probe; printf '%s' "${_rw_wrapper-unset}"; } ) \
   && [ "$_wr_leak" = "unset" ]; then
  ok 'structural (#3367): a `local` in a shell function does not leak to the caller (measured), so the wrapper path run_wrapper resolves cannot be steered by anything outside it'
else
  bad "structural (#3367): a function-local leaked to the caller (got '${_wr_leak:-?}'), so run_wrapper's wrapper path is effectively global again and any caller could redirect it"
fi
# CAPTURED ONCE AND MATCHED IN PURE BASH -- no pipe, no exit status to lose (roborev job 59).
# This was `<view> | grep -q <needle>` under `set -uo pipefail`, which is the inversion documented on
# THIS issue: `grep -q` exits at the first match, SIGPIPEs the writer, and pipefail takes the 141 --
# so a MATCH is reported as a non-match and the forbidden global rides through. Measured here: 30/30
# detected, because the needle happens to sit at byte 175k of a 201k stream, so the reader consumes
# nearly all of it before exiting. That is fail-open BY POSITION -- the issue's own instance inverted
# 39 times in 40 with an early match -- and "safe at today's byte offset" is not a property worth
# depending on. Same remedy #3416 applied to the classifier scan.
#
# The needle is split because the prose above names the retired global while explaining why it is
# gone; comment lines are dropped for the same reason. (Fifth self-reference of this shape in this
# PR -- an artifact describing a rule matching the rule.)
_wr_rwp_view=$(grep -v '^[[:space:]]*#' "$TEST_SELF" || true)
case "$_wr_rwp_view" in
  *'RUN_WRAPPER''_PATH'*)
    _wr_bad="$_wr_bad the-scratch-path-global-is-back;" ;;
esac
if ! grep -qE '^[[:space:]]*local _rw_wrapper="\$WRAPPER_REAL"' "$TEST_SELF"; then
  _wr_bad="$_wr_bad run_wrapper-no-longer-defaults-its-path-to-a-local-seeded-from-WRAPPER_REAL;"
fi
if [ -z "$_wr_bad" ]; then
  ok 'structural (#3367): there is no mutable WRAPPER global, and the scratch-copy override is read at exactly one site (run_wrapper) — a doctrine scan has no mutable path available to it, so the order dependence is absent by construction rather than policed'
else
  bad "structural (#3367): the mutable wrapper path is back —$_wr_bad. Doctrine scans would again be judged against whichever file the last gate-mock case left behind (this is #3367; do not re-introduce a guard for it, remove the global)"
fi
# WRAPPER_REAL is the one path anything reads, and the SHELL keeps it immutable — asked of the shell
# rather than inferred from the declaration text, because `export`/`+=`/`printf -v` all evade a grep
# for assignment spellings (roborev job 37).
if ( WRAPPER_REAL=/nonexistent/decoy ) 2>/dev/null; then
  bad 'structural (#3367): WRAPPER_REAL could be reassigned in a subshell, so the readonly declaration is not in force'
else
  ok 'structural (#3367): a reassignment of WRAPPER_REAL is REFUSED by the shell (measured, not inferred from the declaration text)'
fi
if [ "$WRAPPER_REAL" = "$SCRIPT_DIR/../flow/roborev-review.sh" ]; then
  ok 'structural (#3367): WRAPPER_REAL still names the real wrapper, so the immutable path is also the CORRECT path'
else
  bad "structural (#3367): WRAPPER_REAL does not name the real wrapper — it is immutable but wrong: $WRAPPER_REAL"
fi


# ===== ABSENCE IS A FAIL, AND THE WAIVER IS THE ONLY WAY PAST IT =====
_pc_start=$(grep -nE '^roborev_check_prompt_content\(\) \{' "$CHECKS_FILE" | head -1 | cut -d: -f1)
_pc_end=$(awk -v s="${_pc_start:-0}" 'NR>s && /^}/ {print NR; exit}' "$CHECKS_FILE")
_pc_body=$(sed -n "${_pc_start:-1},${_pc_end:-1}p" "$CHECKS_FILE")
_pc_exec=$(printf '%s\n' "$_pc_body" | grep -v '^[[:space:]]*#')
if printf '%s\n' "$_pc_exec" | grep -qF 'roborev_absence_waiver_lookup "${RANGE_BASE_SHA:-}" "${HEAD_SHA:-}" "${JOB:-}"' \
  && [ "$(printf '%s\n' "$_pc_exec" | grep -cF 'roborev_absence_waiver_lookup')" -eq 1 ] \
  && printf '%s\n' "$_pc_body" | grep -qF 'PROMPT_CONTENT="WAIVED ('; then
  ok 'structural: the waiver is looked up EXACTLY ONCE, inside the absence branch, so it can excuse only that verdict (constraint (c))'
else
  bad 'structural: the absence waiver is not confined to the absence branch — a lookup anywhere else could excuse a verdict the ruling says it may never touch (#3312 ruling (4c))'
fi
# THE WAIVER TOKEN IS DISTINCT FROM `PASS`, so no reader grepping `prompt-content: PASS` counts a waived
# run as a certification — the false-assurance shape this whole issue is about.
if ! grep -qE 'PROMPT_CONTENT="PASS \(.*[Ww]aive' "$CHECKS_FILE" \
  && grep -qF 'WAIVED|SKIP|NOTICE' "$WRAPPER_REAL"; then
  ok 'structural: a waived absence reports the DISTINCT token WAIVED, and the grammar recognises it'
else
  bad 'structural: a waived absence is spelled as a PASS, or WAIVED is outside the block grammar — either way a reader cannot tell a waived run from a certified one (#3312 ruling (4))'
fi
# ===== THE PIPED-`grep -q` SWEEP, RECORDED SO IT DOES NOT HAVE TO BE REDONE (#3387, #3626 round 3) =====
# `writer | grep -q needle` under this file's `pipefail` is fail-open BY BYTE POSITION: `grep -q`
# exits at its first match, SIGPIPEs the writer, and the pipeline status becomes 141, so a MATCH is
# reported as a NON-match. Polarity decides what that costs. NEGATED clauses (`! … | grep -q`) and
# `… | grep -q … && flag=0` report a real violation as clean — FAIL-OPEN, and those are fixed by
# extracting the writer's output to a file and grepping the FILE. Plain positive clauses inside an
# `&&` chain only lose an `ok` to a `bad` — a FALSE RED, loud and self-correcting.
# Sweep of this file at #3626 round 3: 31 piped sites, 13 fail-open by polarity. Fixed: the three
# WAIVER-side guards over whole-file writers (this block, the sole-content Markdown-recogniser scan,
# and the marker-form-in-the-shell scan) — their DEFERRAL siblings were already hardened, and an
# asymmetry inside one file reads as deliberate. The remaining 10 fail-open sites all write a single
# extracted function body, one `case` line or a normally-EMPTY `grep -n` result — bytes, not
# kilobytes — so the writer completes in one non-blocking write and no SIGPIPE is reachable. They are
# named in the #3626 PR body rather than churned here. A NEW site must be classified the same way.
# THE AFFIRMATION BACKSTOP ADMITS `WAIVED` ONLY ON COMPLETE PROVENANCE, and gates on the provenance
# rather than on which key carries it: a key-scoped exemption is the shape the ruling deleted.
# EXTRACTED TO A FILE, NEVER PIPED INTO `grep -q` (#3387). The third clause is NEGATED, which is the
# fail-open polarity: `grep -q` exits at its FIRST match, SIGPIPEs the writer, this file's `pipefail`
# takes the 141, and a MATCH is therefore reported as a NON-match — so a reintroduced per-key
# `prompt-content` escape hatch, the exact shape the #3312 ruling deleted, would read as absent. The
# two positive clauses are extracted with it: they are only false-red capable, but leaving them piped
# would put two spellings of one rule side by side, which is how a later edit "fixes" the wrong half.
# This is the WAIVER's copy of the guard whose DEFERRAL sibling is hardened at `_dfe_exec` below —
# an asymmetry inside one file reads as deliberate and invites exactly that mistake.
_aff_start=$(grep -nF 'for keyed in "push-assert=$PUSH_ASSERT"' "$WRAPPER_REAL" | head -1 | cut -d: -f1)
_aff_body_f="$tmp/aff-body.txt"
sed -n "${_aff_start:-1},$(( ${_aff_start:-1} + 30 ))p" "$WRAPPER_REAL" >"$_aff_body_f"
if [ ! -s "$_aff_body_f" ]; then
  bad 'structural: the affirmation-backstop block could not be located in the wrapper, so its provenance terms were not checked — a failure to measure, not a measurement'
elif grep -qF 'ROBOREV_WAIVER_SCOPE:-}" = "base=${RANGE_BASE_SHA:-} head=${HEAD_SHA:-} job=${JOB:-}"' "$_aff_body_f" \
  && grep -qF 'ROBOREV_WAIVER_STATE:-}" = "granted"' "$_aff_body_f" \
  && ! grep -qF 'det_key" = "prompt-content"' "$_aff_body_f"; then
  ok 'structural: WAIVED is admitted only with a complete, sha-matching provenance, and the gate is not key-scoped'
else
  bad 'structural: the affirmation backstop admits WAIVED without checking its provenance, or reintroduces a per-key escape hatch (#3312 ruling (4))'
fi
# ===== FENCED REGIONS ARE SKIPPED, IN THE SCANNER (#3312 job 28) =====
# A fence preserves column zero, so the anchor alone did not stop a populated marker quoted inside one from
# granting — the accidental bypass most likely to occur, since a fence is how a human documents a syntax.
# The state machine must live in the SCANNER (one implementation of the parse) and must track both fence
# characters; a shell-side copy is how the in-band channel of job 26 came back.
_sole_ok=1
# THE STEM IS A PARAMETER, so BOTH marker kinds (the absence waiver and the #3626 findings
# deferral) inherit this rule BY CALL. A second copy of it for the second kind would be a second
# place for the channel rule to diverge, and a divergence here is an authorization bypass. It is a
# STEM rather than a prefix-with-space since roborev round 2: an attempt is the stem plus whitespace
# OR END OF LINE, so a comment that is exactly the stem is MALFORMED instead of silently NONE.
grep -qF 'def sole_marker_line(body, stem):' "$SCAN_TOOL" || _sole_ok=0
grep -qF 'if len(nonblank) != 1:' "$SCAN_TOOL" || _sole_ok=0
grep -qF 'ONE DECISION, NO PARSE' "$SCAN_TOOL" || _sole_ok=0
# AND NO MARKDOWN RECOGNISER MAY RETURN. Four were tried and superseded; reintroducing one would restore the
# unbounded game of deciding "data or control?" inside a grammar the comment author controls.
# EXECUTABLE LINES ONLY: the comment block RECORDS the four superseded recognisers by name (including
# HTML <pre>), and that history is the durable artifact — scanning prose would make writing it down a
# violation, which is the same mistake as the job-18 census assert.
# EXTRACTED TO A FILE, NEVER PIPED INTO `grep -q` (#3387). The polarity is fail-open: the whole
# point is that a MATCH sets `_sole_ok=0`, and `grep -q` exits at its first match, SIGPIPEs the
# upstream `grep -v`, and `pipefail` hands the pipeline a 141 — so the `&&` never fires and a
# REINTRODUCED Markdown recogniser reads as absent. Today the guard finds nothing (the recogniser
# was deleted), which is precisely why the fail-open leg is invisible until it matters.
_sole_scan_exec="$tmp/sole-scan-exec.txt"
grep -vE '^[[:space:]]*#' "$SCAN_TOOL" >"$_sole_scan_exec" || true
grep -qE 'FENCE_CHARS|fence_run|def .*fence|<pre>|lstrip\("`"\)' "$_sole_scan_exec" && _sole_ok=0
grep -qE 'FENCE_CHARS|fence_run' "$ORACLES" && _sole_ok=0
if [ "$_sole_ok" -eq 1 ]; then
  ok 'structural: an authorization must be the SOLE NONBLANK CONTENT of its comment, decided without parsing Markdown'
else
  bad 'structural: the sole-content rule is missing, or a Markdown recogniser (fence/HTML) came back — the class was closed by REMOVING the shared channel, not by extending the parser, and four successive recognisers were superseded proving it (#3312 job 29)'
fi
# ===== THE THREAT-MODEL BOUNDARY IS STATED, ON EVERY SURFACE (#3312) =====
# Five consecutive rounds landed in the waiver's authorization path, so the boundary is recorded to get the
# NEXT finding triaged rather than patched: a hostile INVOKER is out of model by construction (they control
# the process), while a NON-INVOKER bypass or an ACCIDENTAL one is a defect. An unstated boundary is how
# "the invoker can bypass this" becomes another round — and how the opposite error, treating a real
# third-party hole as unpatchable, would creep in.
_tm_missing=""
for _f in "$ORACLES" "$WRAPPER_REAL"; do
  grep -qF 'A HOSTILE INVOKER' "$_f" || _tm_missing="$_tm_missing $(basename "$_f")"
done
grep -qF 'the INVOKER can bypass this' "$ORACLES" || _tm_missing="$_tm_missing oracles-triage-rule"
grep -qF 'top-level PR comments only' "$ORACLES" \
  || grep -qF 'TOP-LEVEL PR COMMENTS ONLY' "$ORACLES" || _tm_missing="$_tm_missing comment-channel-residual"
if [ -z "$_tm_missing" ]; then
  ok 'structural: the waiver threat model, its triage rule and the comment-channel residual are stated in code'
else
  bad "structural: the waiver threat model is not stated —$_tm_missing. Without the boundary, an out-of-model 'the invoker can bypass this' finding gets patched instead of recorded, and a real non-invoker hole could be waved away as unpatchable (#3312)"
fi
# ===== NO COMMENT-PROVENANCE DECISION FROM A FLATTENED TEXT STREAM (#3312 job 26) =====
# The class, not the instance: an in-band author channel is forgeable by the very data it labels, so the
# association must come from the JSON structure. This asserts (a) the wrapper asks for raw `--json`
# without a `--jq` flattening program, (b) the decision is delegated to the structured scanner, and
# (c) no SOH-style delimiter or comment-line loop survives in the oracles.
_prov_ok=1
grep -qF 'gh pr view --json comments 2>/dev/null' "$ORACLES" || _prov_ok=0
grep -qF 'python3 "$WAIVER_SCAN_TOOL"' "$ORACLES" || _prov_ok=0
grep -q 'u0001' "$ORACLES" && _prov_ok=0
grep -qF 'author.login' "$ORACLES" && _prov_ok=0
if [ "$_prov_ok" -eq 1 ]; then
  ok 'structural: comment provenance is decided from the JSON structure, with no in-band author channel'
else
  bad 'structural: the waiver still associates author and body through a flattened text stream (or asks jq to flatten them) — a comment body can forge its own author record, which defeats the allowlist (#3312 job 26)'
fi
# AND THE SCANNER OWNS THE MARKER FORM: two implementations of the pattern would drift, and the shell
# having one at all is what made the text channel possible.
# EXTRACTED TO A FILE, NEVER PIPED INTO `grep -q` (#3387). This is the WAIVER marker's copy of the
# guard whose DEFERRAL sibling is hardened at `_dfe_exec`, and the negated clause is the fail-open
# one: `grep -q` exits at its first match, SIGPIPEs the `grep -v`, `pipefail` takes the 141, and the
# `!` turns a real shell-side copy of the marker form into "absent" — i.e. a second implementation of
# the channel rule, which is an authorization bypass (#3312 job 26), would report clean. The oracles
# file's non-comment output is ~26 KB (it grew ~32% on #3626) against a 64 KB pipe buffer: at that
# size the writer normally finishes, so the fail-open is currently unlikely rather than routine —
# but the verdict is resting on a pipe-buffer size, which is the whole reason for the rule.
_tm_oracles_exec="$tmp/tm-oracles-exec.txt"
grep -v '^[[:space:]]*#' "$ORACLES" >"$_tm_oracles_exec" || true
if ! grep -qF 'roborev-waive: prompt-content-absent' "$_tm_oracles_exec" \
  && grep -qF 'roborev-waive: prompt-content-absent' "$SCAN_TOOL"; then
  ok 'structural: the marker form is expressed once, in the structured scanner'
else
  bad 'structural: the marker pattern exists in the shell as well as the scanner — two implementations drift, and a shell-side parse is how the in-band channel returns (#3312 job 26)'
fi
# ===== AUTHORIZATION IS ENFORCED, AND THE RESIDUAL IS SCOPED TO WHAT REMAINS TRUE (#3312 job 25) =====
# This assert used to forbid ANY author check, on the reasoning that one could not distinguish worker from
# owner on a shared login. That reasoning conflated "cannot enforce perfectly" with "cannot enforce at
# all", and the hole it left was that ANY commenter on a public repository could grant a waiver. So the
# assert is inverted where it was wrong and kept where it was right: the allowlist comparison MUST exist,
# and the disclaimer MUST be scoped to "which allowlisted human", never to authorship in general.
_allow_ok=1
grep -qF 'ROBOREV_WAIVER_AUTHORS=' "$ORACLES" || _allow_ok=0
# The allowlist VALUE is visible in the shell; the DECISION lives in the structured scanner (job 26), so
# the author is compared against it where author and body are still separate fields.
grep -qF '"$ROBOREV_WAIVER_AUTHORS"' "$ORACLES" || _allow_ok=0
grep -qF 'if author not in allowlist' "$SCAN_TOOL" || _allow_ok=0
grep -qF 'return "unauthorized"' "$SCAN_TOOL" || _allow_ok=0
grep -qF 'unauthorized|stale|malformed|none' "$ORACLES" || _allow_ok=0
if [ "$_allow_ok" -eq 1 ]; then
  ok 'structural: the waiver author is authorized against an explicit allowlist, with its own UNAUTHORIZED state'
else
  bad 'structural: the waiver has no author allowlist — on a public repository any commenter could copy the base/head/job from a failing block and grant one (#3312 job 25)'
fi
# NOT ENV-OVERRIDABLE — AND NEITHER IS ITS ENFORCER (#3312 job 25, extended by job 27).
# THE CONSTRAINED PARTY MUST NOT CHOOSE ITS OWN ENFORCER: hardening a check while leaving its INVOCATION
# configurable moves the hole instead of closing it. The allowlist VALUE was covered first; the scanner
# PATH was then left env-settable, which let an invoker point it at a script printing `state=granted`. Both
# halves are asserted here, and the path must be a literal repository-relative resolution with no `${…:-…}`.
_enf_bad=""
grep -qE 'ROBOREV_WAIVER_AUTHORS="?\$\{?ROBOREV_WAIVER_AUTHORS' "$ORACLES" && _enf_bad="$_enf_bad allowlist-value-env-derived"
grep -qE 'WAIVER_SCAN_TOOL="?\$\{WAIVER_SCAN_TOOL' "$ORACLES" && _enf_bad="$_enf_bad scanner-path-env-derived"
grep -qF 'WAIVER_SCAN_TOOL="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/roborev-waiver-scan.py"' "$ORACLES" \
  || _enf_bad="$_enf_bad scanner-path-not-literally-repo-relative"
if [ -z "$_enf_bad" ]; then
  ok 'structural: neither the allowlist nor its enforcer is env-derived (the scanner path is resolved from the wrapper own directory)'
else
  bad "structural: the waiver protection is configurable by its invoker —$_enf_bad. An override is settable by the very party being constrained, so the constrained party would choose its own enforcer (#3312 job 25/27)"
fi
# AND NO TEST SEAM MAY REINTRODUCE IT: a case needing a different scanner substitutes the FILE in a
# scratch copy of scripts/flow/, never a path variable — a test-only hatch is one more thing an invoker sets.
# THE NEEDLE IS SPLIT so this assert cannot match ITSELF — a self-matching grep would fire on the very
# line that forbids the seam, which is a guard that can only ever be red.
_seam_needle="WAIVER_SCAN_TOOL""_OVERRIDE"
if grep -qF "$_seam_needle" "$TEST_SELF"; then
  bad 'structural: the harness carries a scanner-path override — substitute the scanner FILE in a scratch copy instead, so production keeps one literal resolution (#3312 job 27)'
else
  ok 'structural: the harness substitutes the scanner file rather than redirecting its path'
fi
# ===== THE DEFERRAL HAS NO CHANNEL THE REVIEWED PARTY CAN WRITE IN ITS OWN NAME (#3626) =====
# THE TRAP THIS PINS, and it is the whole design problem of #3626: the obvious fix — let a lane mark
# findings deferred so the tool passes — HANDS THE CONSTRAINED PARTY THE POWER TO SATISFY ITS OWN
# CONSTRAINT. A `--defer-finding` flag, a deferral file in the worktree and an env var are each that
# shape, so all three are asserted ABSENT here. Behavioural cases cover only the channels someone
# already thought of; this covers the class.
_dfs_bad=""
# (a) NO CLI FLAG. Read from the wrapper's OWN option parser, so a `--defer`-shaped string in the
#     header doctrine or in `--help` (where the marker form legitimately lives) can neither satisfy
#     nor break this assert.
# EXTRACTED TO A FILE, NEVER PIPED INTO `grep -q` (#3387, and this suite's own documented
# instance): `grep -q` exits at the first match, SIGPIPEs the writer, and `pipefail` takes the
# 141 — so a MATCH is reported as a NON-match, fail-open BY BYTE POSITION. Measured here: the
# reset loop below reported the LAST variable missing while the file plainly contained it.
_dfs_opts="$tmp/dfs-opts.txt"
awk '/^while \[ \$# -gt 0 \]; do/ { inb = 1 } inb { print } inb && /^done$/ { exit }' "$WRAPPER_REAL" >"$_dfs_opts"
if [ ! -s "$_dfs_opts" ]; then
  _dfs_bad="$_dfs_bad option-parser-not-locatable"
elif grep -qiE -- '--defer' "$_dfs_opts"; then
  _dfs_bad="$_dfs_bad deferral-cli-flag"
fi
# (b) NO ENVIRONMENT CHANNEL. Two halves: no deferral variable is ever DERIVED from the environment,
#     and the check RESETS every field the verdict reads before it decides anything — so an exported
#     value cannot stand in for one this run established. The reset is the mechanism; the grep is the
#     backstop.
for _dfs_f in "$WRAPPER_REAL" "$ORACLES" "$CHECKS_FILE"; do
  if grep -qE '(ROBOREV_DEFERRAL_[A-Z_]+|DEFERRAL_REPORT)="?\$\{(ROBOREV_DEFERRAL|DEFERRAL_REPORT)' "$_dfs_f"; then
    _dfs_bad="$_dfs_bad deferral-env-derived-in-$(basename "$_dfs_f")"
  fi
done
_dfs_reset="$tmp/dfs-reset.txt"
awk '/^roborev_check_findings_deferral\(\) \{/ { inb = 1 } inb { print } inb && /RECHECK_JOB:-}" \]/ { exit }' \
  "$CHECKS_FILE" >"$_dfs_reset"
if [ ! -s "$_dfs_reset" ]; then
  _dfs_bad="$_dfs_bad deferral-check-prologue-not-locatable"
else
  for _dfs_v in STATE AUTHOR SCOPE REASON DETAIL ISSUES COUNT OBSERVED_COUNT; do
    grep -qE "^  ROBOREV_DEFERRAL_$_dfs_v=\"\"$" "$_dfs_reset" \
      || _dfs_bad="$_dfs_bad unreset-ROBOREV_DEFERRAL_$_dfs_v"
  done
  grep -qE '^  DEFERRAL_REPORT=""$' "$_dfs_reset" || _dfs_bad="$_dfs_bad unreset-DEFERRAL_REPORT"
fi
# (c) NO FILE CHANNEL. A deferral read from the worktree is the same hole as the flag, plus the
#     daemon-vs-root class that has already bitten three lanes on a config the tool reads from root.
for _dfs_f in "$WRAPPER_REAL" "$ORACLES" "$CHECKS_FILE" "$SCAN_TOOL"; do
  if grep -qE '\.roborev[-_]?defer|defer(red|rals?)[-_.](txt|json|toml|list|file)' "$_dfs_f"; then
    _dfs_bad="$_dfs_bad deferral-file-channel-in-$(basename "$_dfs_f")"
  fi
done
if [ -z "$_dfs_bad" ]; then
  ok 'structural (#3626): a deferral cannot be asserted by a flag, a file or an environment variable — the authorization is a PR comment only, and the check resets every field the verdict reads'
else
  bad "structural (#3626): the deferral has a channel the reviewed party can write in its own name —$_dfs_bad. A worker could then clear its own findings, which is the constrained party satisfying its own constraint (#3312)"
fi
# ===== ONE ENFORCER, ONE MARKER FORM, TWO KINDS SELECTED EXPLICITLY (#3626) =====
# The channel rules are inherited BY CALL, not by copy: the deferral asks the SAME scanner, resolved
# from the SAME literal path, selecting its kind explicitly. A second scanner (or a second copy of the
# marker pattern in the shell) would be a second place for the channel rule to diverge, and a
# divergence in a channel rule is an authorization bypass — which is exactly how the in-band author
# channel came back once already (#3312 job 26).
_dfe_bad=""
grep -qF 'python3 "$WAIVER_SCAN_TOOL" findings-deferral' "$ORACLES" || _dfe_bad="$_dfe_bad deferral-does-not-call-the-one-scanner"
grep -qF 'python3 "$WAIVER_SCAN_TOOL" prompt-content-absent' "$ORACLES" || _dfe_bad="$_dfe_bad waiver-kind-not-named-explicitly"
grep -qF 'roborev-defer: findings' "$SCAN_TOOL" || _dfe_bad="$_dfe_bad marker-form-absent-from-the-scanner"
# THE MARKER FORM MAY NOT LIVE IN THE SHELL. Executable lines only: the comment blocks in these files
# describe the mechanism, and scanning prose would make writing it down a violation — the same mistake
# as the job-18 census assert.
# EXTRACTED TO A FILE, NEVER PIPED INTO `grep -q` (#3387, and this suite's own measured instance
# documented at the `_dfs_opts` extraction above): `grep -q` exits at its FIRST match, SIGPIPEs the
# upstream writer, and this file's `pipefail` takes the 141 — so on a MATCH the pipeline reports a
# NON-match and a real violation reads as clean, fail-open BY BYTE POSITION. The oracles file's
# non-comment output is ~500 lines carrying several 200-400 character detail strings, i.e. tens of
# kilobytes against a 64 KB pipe buffer, so this is not a theoretical margin.
_dfe_exec="$tmp/dfe-exec.txt"
for _dfe_f in "$ORACLES" "$CHECKS_FILE"; do
  grep -v '^[[:space:]]*#' "$_dfe_f" >"$_dfe_exec" || true
  if grep -qF 'roborev-defer' "$_dfe_exec"; then
    _dfe_bad="$_dfe_bad marker-form-in-$(basename "$_dfe_f")"
  fi
done
# AND NO TEST SEAM MAY INTRODUCE A SECOND ENFORCER PATH. The needle is SPLIT so this assert cannot
# match its own line — a self-matching grep is a guard that can only ever be red.
_dfe_seam="DEFERRAL_SCAN""_TOOL"
for _dfe_f in "$TEST_SELF" "$WRAPPER_REAL" "$ORACLES" "$CHECKS_FILE"; do
  if grep -qF "$_dfe_seam" "$_dfe_f"; then
    _dfe_bad="$_dfe_bad second-scanner-path-in-$(basename "$_dfe_f")"
  fi
done
if [ -z "$_dfe_bad" ]; then
  ok 'structural (#3626): the deferral calls the ONE scanner at the ONE literal path, names its kind explicitly, and the marker form exists only there — no second enforcer and no shell-side parse'
else
  bad "structural (#3626): the deferral does not share the waiver enforcer —$_dfe_bad. Two implementations of a channel rule drift, and a drift in a channel rule is an authorization bypass (#3312 job 26/27)"
fi
# ===== ONE COUPLED GRANTED STATE, READ BY ALL THREE GATES (#3626) =====
# `DEFERRED` is non-failing ONLY when the oracle granted, and the grammar scan, the `findings:` gate
# and the affirmation backstop must read ONE state rather than each deciding for itself: two tests of
# "was it granted?" are two things that can drift apart. This pins that the admission is decided once
# and that the findings gate keeps BOTH halves — the token-exact `NONE` requirement (#3564, whose own
# structural case is (fd7)) and the coupling to that single state.
_dfc_bad=""
[ "$(grep -cE '^deferral_admits=0$' "$WRAPPER_REAL")" -eq 1 ] || _dfc_bad="$_dfc_bad admission-not-initialised-once"
[ "$(grep -cE '^  deferral_admits=1$' "$WRAPPER_REAL")" -eq 1 ] || _dfc_bad="$_dfc_bad admission-not-decided-in-exactly-one-place"
# EXECUTABLE READS ONLY, and there are exactly FOUR: the initialisation, the one decision, the
# grammar scan's `findings`-scoped arm and the findings gate. A FIFTH would be a third gate reading
# the state — which is how the unconfined admission got into the affirmation backstop (job 225).
# Extracted to a FILE and counted there: `grep -c` over a pipe would lose its status to SIGPIPE.
_dfc_reads="$tmp/dfc-reads.txt"
grep -vE '^[[:space:]]*#' "$WRAPPER_REAL" >"$_dfc_reads" || true
[ "$(grep -cF 'deferral_admits' "$_dfc_reads")" -eq 4 ] \
  || _dfc_bad="$_dfc_bad admission-reads=$(grep -cF 'deferral_admits' "$_dfc_reads")-expected-4"
grep -qF 'if [ "$failed" -eq 0 ] && [ "${FINDINGS%% *}" != NONE ] && [ "$findings_deferred" -ne 1 ]; then' "$WRAPPER_REAL" \
  || _dfc_bad="$_dfc_bad findings-gate-lost-a-half"
grep -qF 'if [ "$deferral_admits" -eq 1 ] && [ "${FINDINGS%% *}" = DEFERRED ]; then' "$WRAPPER_REAL" \
  || _dfc_bad="$_dfc_bad findings-deferral-admission-not-token-exact-or-not-coupled"
# The admission's own terms: a granted state is not enough — the provenance must be complete, the
# scope must equal THIS run's, the counts must match, and the mode must be recheck.
_dfc_admit="$tmp/dfc-admit.txt"
awk '/^deferral_admits=0$/ { inb = 1 } inb { print } inb && /^fi$/ { exit }' "$WRAPPER_REAL" >"$_dfc_admit"
[ -s "$_dfc_admit" ] || _dfc_bad="$_dfc_bad admission-block-not-locatable"
for _dfc_term in 'ROBOREV_DEFERRAL_STATE:-}" = "granted"' 'RECHECK_JOB:-}" ]' 'ROBOREV_DEFERRAL_AUTHOR:-}" ]' \
  'ROBOREV_DEFERRAL_REASON:-}" ]' 'ROBOREV_DEFERRAL_ISSUES:-}" ]' \
  'ROBOREV_DEFERRAL_COUNT:-}" = "${ROBOREV_DEFERRAL_OBSERVED_COUNT:-}"' \
  'ROBOREV_DEFERRAL_SCOPE:-}" = "base=${RANGE_BASE_SHA:-} head=${HEAD_SHA:-} job=${JOB:-}"'; do
  grep -qF -- "$_dfc_term" "$_dfc_admit" || _dfc_bad="$_dfc_bad admission-missing-term:${_dfc_term%%:*}"
done
if [ -z "$_dfc_bad" ]; then
  ok 'structural (#3626): the deferral admission is decided ONCE, on complete provenance, a matching scope, equal counts and recheck mode, and all three gates read that one state'
else
  bad "structural (#3626): the deferral admission is not one coupled state —$_dfc_bad. A second derivation of 'was it granted?' can drift from the first, and a DEFERRED token that no authorization backs is indistinguishable from an authorized one to every reader of the block"
fi
# ===== THE DEFERRAL IS CONFINED TO THE ROBOREV VERDICT (#3626) =====
# IT IS NOT A GENERAL "OVERRIDE ANY CHECK" MECHANISM, and the gate of record must not learn to read one:
# `scripts/agent-gate.sh` is out of scope for this change by owner ruling (three lanes are live on it),
# and no gate component may consume a deferral marker. Asserted rather than left to a one-time
# `git diff` review, because the next person to want a broader override will look for a precedent.
_dfg_gate="$SCRIPT_DIR/../agent-gate.sh"
if [ ! -f "$_dfg_gate" ]; then
  bad "structural (#3626): the agent gate is not at $_dfg_gate, so the confinement claim could not be checked"
elif grep -qE 'roborev-defer|ROBOREV_DEFERRAL|deferral_admits' "$_dfg_gate"; then
  bad 'structural (#3626): the agent gate reads a deferral marker or its state — the deferral is confined to the roborev wrapper verdict and must never become a general "override any check" mechanism (#3626 scope fence)'
else
  ok 'structural (#3626): no gate component consumes a deferral — the mechanism is confined to the roborev wrapper verdict'
fi
# ===== `findings:` NEVER REPORTS `NONE` ON ACCOUNT OF A DEFERRAL (#3626) =====
# `NONE` stays reachable ONLY from the job record's structured `verdict` letter, so nobody grepping
# `findings: NONE` reads a deferred run as a clean review. Asserted at the assignment site, because
# the behavioural cases can only show it for the shapes they fixture.
# THE `NONE` ASSIGNMENT SITES ARE EXTRACTED TO A FILE, not piped into a negated `grep -q` (#3387).
# The polarity is the fail-open one: a SIGPIPE-141 from the downstream `grep -q` reads as "no
# deferral spelling found", i.e. this change's OWN never-`NONE` property would report clean exactly
# when it is violated.
_dfn_none="$tmp/dfn-none-sites.txt"
grep -nF 'FINDINGS="NONE"' "$CHECKS_FILE" >"$_dfn_none" || true
if grep -qF 'FINDINGS="DEFERRED (' "$CHECKS_FILE" \
  && ! grep -qi 'defer' "$_dfn_none"; then
  ok 'structural (#3626): a granted deferral assigns the DISTINCT token DEFERRED, and no deferral path can assign NONE'
else
  bad 'structural (#3626): a deferral can reach findings: NONE, or no longer reports the distinct DEFERRED token — either way a reader grepping for a clean review would count a deferred one (#3626)'
fi
# THE SCOPED RESIDUAL IS STATED ON EVERY SURFACE, in its NARROW form.
_resid_missing=""
for _f in "$ORACLES" "$CHECKS_FILE" "$WRAPPER_REAL"; do
  grep -qiF 'WHICH ALLOWLISTED HUMAN' "$_f" || _resid_missing="$_resid_missing $(basename "$_f")"
done
if [ -z "$_resid_missing" ]; then
  ok 'structural: the process-enforcement residual is scoped to "which allowlisted human", on all three surfaces'
else
  bad "structural: the authorship residual is missing or unscoped in —$_resid_missing. An over-broad 'authorship cannot be verified' disclaimer is what justified having no author check at all (#3312 job 25)"
fi
# NO EMITTED DIAGNOSTIC MAY DESCRIBE A RETIRED MECHANISM AS SOMETHING THIS RUN DOES (#3312 job 18, fix 2).
# The retirement of the capture/observer/digest apparatus left prose behind that still described it in the
# present tense, and a diagnostic that names a mechanism the wrapper does not have sends its reader looking
# for behaviour that cannot be there. The scan covers the strings a reader actually SEES — the DETAILS lines
# and the verdict values — and only AFFIRMATIVE spellings: the NOTICE deliberately says "there is no digest
# and no content identity", and a statement of ABSENCE is exactly what must survive.
_diag=$(grep -hnE 'DETAILS\+=\(|^[[:space:]]*(PROMPT_CONTENT|REVIEW_COMPLETED|TIER1|TIER2|FINDINGS|CENSUS_CHECK|CODE_FREE|PUSH_ASSERT|SHA_ASSERT|JOB_RECORD)=' \
  "$WRAPPER_REAL" "$CHECKS_FILE" "$ORACLES" 2>/dev/null || true)
_diag_stale=$(printf '%s\n' "$_diag" | grep -inE 'captur|watcher|watchdog|observer|digest(ed|ing)? (of|the snapshot)|snapshot digest|mid-copy|SNAPSHOT_(DIGEST|BYTES)|bounded read|read the snapshot' || true)
if [ -z "$_diag_stale" ]; then
  ok 'structural: no emitted diagnostic names a retired mechanism (capture/watcher/watchdog/observer/digest) as current behaviour'
else
  bad "structural: an emitted diagnostic describes a mechanism C⁗ deleted — ${_diag_stale%%$'\n'*} (#3312 fix 2)"
fi
_r1=$(grep -nE 'ROBOREV_SNAPSHOT_UNUSABLE_WHY="[^"]*\$\(\[' "$ORACLES" || true)
if [ -z "$_r1" ]; then
  ok 'structural: no cause string is built with an optional command substitution (rider R1: it aborts under set -e)'
else
  bad "structural: a cause string embeds an optional command substitution in a simple assignment, which takes its status and aborts under set -e: ${_r1%%$'\n'*} (#3312 rider R1)"
fi

# THE AFFIRMATIVE-MEASUREMENT SHAPE, at the branch point that remains (CLAUDE.md; #3229 round-10).
# There is no diff-source state machine any more — owner ruling (4) deleted it — so what must hold is
# narrower and stronger: `prompt-content:` has exactly THREE outcomes, each reached by an affirmative
# test, and none of them is a fall-through. PASS requires every code census path to have been FOUND;
# FAIL is the absence; WAIVED is the absence plus a complete human provenance. A `0/0` is still never a
# pass, which is the one case where "nothing to measure" must not read as "measured fine".
if printf '%s\n' "$_pc_body" | grep -qF 'PROMPT_CONTENT="PASS (${#checked_paths[@]}/$census_total code census paths present)"' \
  && printf '%s\n' "$_pc_body" | grep -qF 'PROMPT_CONTENT="FAIL (${#missing_paths[@]}/${#checked_paths[@]} code census paths absent from the prompt)"' \
  && printf '%s\n' "$_pc_body" | grep -qF 'a 0/0 is never a pass' \
  && printf '%s\n' "$_pc_body" | grep -qF 'FAIL (prompt unretrievable'; then
  ok 'structural: prompt-content has exactly its three affirmative outcomes (present PASS / absent FAIL / absent+provenance WAIVED), plus the 0/0 and unretrievable-prompt refusals'
else
  bad 'structural: prompt-content no longer reaches its outcomes affirmatively — a missing absent-FAIL, a missing 0/0 refusal or a missing unretrievable-prompt refusal each turns an unmeasured prompt into a pass (#3312 ruling (4))'
fi
# The matcher must resolve ambiguity from git's rename/copy lines, not positionally. Asserted
# against the matcher body: a future edit that drops the rename-line branch and goes back to
# a bare positional test is exactly blocker 1 reintroduced.
if [ -n "$_matcher_start" ] && [ -n "${_matcher_end:-}" ]; then
  _m_body=$(sed -n "${_matcher_start},${_matcher_end}p" "$ORACLES")
  if printf '%s\n' "$_m_body" | grep -qE '\[ -n "\$from_tok" \] && \[ -n "\$to_tok" \]'; then
    ok 'structural: the matcher resolves a rename/copy header from its from/to path tokens first'
  else
    bad 'structural: the matcher no longer resolves from the rename/copy from/to tokens — ambiguity would be guessed positionally again (#3229 blocker 1)'
  fi
  if printf '%s\n' "$_m_body" | grep -qE '"a/\$want b/"\*'; then
    bad 'structural: the matcher is back to the PREFIX test `case $rest in "a/$want b/"*` — a tracked file named `foo b/x` would make the unrelated path `foo` read PRESENT (#3229 blocker 1)'
  else
    ok 'structural: the retired `"a/$want b/"*` prefix test is gone from the matcher'
  fi
  if printf '%s\n' "$_m_body" | grep -qE 'eq_seen'; then
    ok 'structural: an ambiguous non-rename header is decided by the EQUAL split, not by position'
  else
    bad 'structural: the matcher has no equal-split resolution for an ambiguous header'
  fi
fi
# The census must classify the RAW path. Asserted by the absence of any normalisation call
# in the census loop AND by the `-z` read above: a quoted spelling reaching the extension
# test is exactly blocker F1 (`docs/é notes.md` ⇒ ext `md"` ⇒ CODE).
_census_start=$(grep -nE '^roborev_census\(\) \{' "$ORACLES" | head -1 | cut -d: -f1)
if [ -n "$_census_start" ]; then
  _census_end=$(awk -v s="$_census_start" 'NR>s && /^  \}$/ {print NR; exit}' "$ORACLES")
  # AN UNRESOLVED BOUND IS ITS OWN FAILURE, NEVER A SILENT 1-LINE RANGE. The previous
  # `${_census_end:-$_census_start}` fallback degraded a failed/empty `awk` into a range of
  # ONE line, in which the absence-assert below reads `ok` (nothing to find) while the
  # presence-assert reds — a FLAKY FAIL that names the wrong defect and, in the other
  # direction, an assert satisfied by scanning nothing. OBSERVED once under gate load, on a
  # tree whose `read -r -d ''` is provably present. So the bound is verified before use.
  if [ -z "$_census_end" ] || [ "$_census_end" -le "$_census_start" ]; then
    bad "structural: the census body bounds could not be resolved (start $_census_start, end '${_census_end:-<none>}') — the range asserts below would scan nothing, so this is a failure to measure, not a measurement"
  else
    ok "structural: the census body bounds resolved (lines $_census_start-$_census_end)"
    _census_body=$(sed -n "${_census_start},${_census_end}p" "$ORACLES")
    if printf '%s\n' "$_census_body" | grep -q 'roborev_unquote_path '; then
      bad 'structural: the census normalises inside its own loop — it must read raw paths instead (-z)'
    else
      ok 'structural: the census classifies the RAW path (no unquoting inside the census loop)'
    fi
    if printf '%s\n' "$_census_body" | grep -qF 'read -r -d '; then
      ok 'structural: the census reads NUL-terminated records (a newline-bearing path survives)'
    else
      bad 'structural: the census does not read NUL-terminated records — a newline-bearing path would split'
    fi
  fi
else
  bad 'structural: roborev_census is not defined'
fi

printf '== structural: NO summary value or DETAILS line is emitted un-neutralised ==\n'
# #3229 round 5, blocker 2. Behavioural case (cx6p) proves ONE path is neutralised; only a
# structural assert can pin that EVERY value is, including keys that do not exist yet. A
# per-site escape is a list to keep complete — the next value to grow a path interpolation
# would silently reopen the hole — so the boundary is `emit_kv` + `finish`, and it is
# asserted against the emitting statements themselves.
_em_start=$(grep -nE '^emit_summary\(\) \{' "$WRAPPER_REAL" | head -1 | cut -d: -f1)
_em_end=""
[ -z "$_em_start" ] || _em_end=$(awk -v s="$_em_start" 'NR>s && /^}/ {print NR; exit}' "$WRAPPER_REAL")
if [ -z "$_em_start" ] || [ -z "$_em_end" ]; then
  bad 'structural: could not locate the emit_summary() body to inspect'
else
  # Every executable line in the body is either the block BANNER or an `emit_kv` call.
  # CONTROL FLOW IS ALLOWED, VALUES ARE NOT (#3312): the three snapshot keys are emitted only in snapshot
  # mode, so the body now contains `if`/`else`/`fi`. Those carry no value; what must never appear is a raw
  # `printf` of one, which is asserted separately below.
  _em_raw=$(sed -n "$((_em_start + 1)),$((_em_end - 1))p" "$WRAPPER_REAL" \
    | grep -vE '^[[:space:]]*(#|$)' \
    | grep -vE "^[[:space:]]*emit_kv '" \
    | grep -vE '^[[:space:]]*(if|elif|else|fi|then)\b' \
    | grep -vE '^[[:space:]]*(if|elif) \[' \
    | grep -vF "printf '==== ROBOREV REVIEW SUMMARY ====" || true)
  _em_printfs=$(sed -n "$((_em_start + 1)),$((_em_end - 1))p" "$WRAPPER_REAL" \
    | grep -E '^[[:space:]]*printf' | grep -vF "printf '==== ROBOREV REVIEW SUMMARY ====" || true)
  if [ -n "$_em_printfs" ]; then
    bad "structural: emit_summary printf's a value directly, bypassing the neutralising boundary: ${_em_printfs%%$'\n'*}"
  else
    ok 'structural: emit_summary contains no raw value printf (only the banner)'
  fi
  if [ -z "$_em_raw" ]; then
    ok "structural: every emit_summary value goes through emit_kv (lines $_em_start-$_em_end)"
  else
    bad "structural: emit_summary emits a value WITHOUT emit_kv, so a newline-bearing path could forge a key: ${_em_raw%%$'\n'*}"
  fi
  # 22 emit_kv lines = 21 keys + the terminal `RESULT:`, which goes through the SAME
  # neutralising boundary. Was 23 before #3229's owner ruling removed `census-exclusion:`
  # with its oracle (#3283).
  _em_n=$(sed -n "$((_em_start + 1)),$((_em_end - 1))p" "$WRAPPER_REAL" | grep -cE "^[[:space:]]*emit_kv '" || true)
  if [ "${_em_n:-0}" -ge 22 ]; then
    ok "structural: all $_em_n block lines (21 keys + RESULT:) are emitted through the neutralising boundary"
  else
    bad "structural: only ${_em_n:-0} emit_kv call(s) in emit_summary — the block has 21 keys plus RESULT:, so some are emitted another way"
  fi
fi
if grep -qE '^[[:space:]]*roborev_safe_line "\$2"' "$WRAPPER_REAL"; then
  ok 'structural: emit_kv neutralises its value before printing it'
else
  bad 'structural: emit_kv does not call roborev_safe_line — the boundary is decorative'
fi
# DETAILS reach the SAME stdout a reader greps for `^RESULT: `, so the bulk
# `printf '%s\n' "${DETAILS[@]}"` (which prints a newline-bearing entry as several lines)
# must be gone, replaced by a per-entry neutralised print.
if grep -qE 'printf .%s..n. "\$\{DETAILS\[@\]\}"' "$WRAPPER_REAL"; then
  bad 'structural: finish still bulk-prints "${DETAILS[@]}" — a newline-bearing DETAILS entry would span lines and could forge a RESULT: line'
else
  ok 'structural: DETAILS are not bulk-printed (each entry is neutralised individually)'
fi
_fin_start=$(grep -nE '^finish\(\) \{' "$WRAPPER_REAL" | head -1 | cut -d: -f1)
_fin_end=""
[ -z "$_fin_start" ] || _fin_end=$(awk -v s="$_fin_start" 'NR>s && /^}/ {print NR; exit}' "$WRAPPER_REAL")
if [ -n "$_fin_start" ] && [ -n "$_fin_end" ] \
  && sed -n "${_fin_start},${_fin_end}p" "$WRAPPER_REAL" | grep -q 'roborev_safe_line'; then
  ok "structural: finish neutralises every DETAILS line (lines $_fin_start-$_fin_end)"
else
  bad 'structural: finish does not neutralise DETAILS lines'
fi

printf '== structural: the authorization-keyword denylist lives at the TWO emit boundaries ==\n'
# ROBOREV ROUND 4 (job 230). The finding named ONE field — the GitHub login in `unauthorized_detail` —
# and the defect is a CLASS: any externally-sourced value can carry a marker keyword into the block (a
# login; `gh issue view`'s stdout and stderr, which reach `deferral:` through ROBOREV_ISSUE_DETAIL; the
# argv-sourced allowlist; any key added later). A per-site escape is a list to keep complete, which is
# the same argument that put control-character neutralising in `emit_kv`/`finish` — so this is asserted
# STRUCTURALLY: the denylist must be INSIDE the two renderers and NOWHERE ELSE, or the next value to
# grow an interpolation silently reopens it.
_rd_bad=""
_rl_start=$(grep -nE '^roborev_safe_line\(\) \{' "$WRAPPER_REAL" | head -1 | cut -d: -f1)
_rl_end=""
[ -z "$_rl_start" ] || _rl_end=$(awk -v s="$_rl_start" 'NR>s && /^}/ {print NR; exit}' "$WRAPPER_REAL")
if [ -z "$_rl_start" ] || [ -z "$_rl_end" ]; then
  bad 'structural: could not locate roborev_safe_line() to inspect the keyword denylist — a failure to measure, not a measurement'
else
  sed -n "${_rl_start},${_rl_end}p" "$WRAPPER_REAL" | grep -q 'ROBOREV_MARKER_REDACTION' \
    || _rd_bad="$_rd_bad wrapper-boundary-does-not-redact"
  # The word boundary is part of the rule, not an optimisation: without it the scanner's own file name
  # is mangled in the `waiver: UNAVAILABLE (... tool: <path>)` cause (case wv31).
  sed -n "${_rl_start},${_rl_end}p" "$WRAPPER_REAL" | grep -qF '[^a-zA-Z]|$' \
    || _rd_bad="$_rd_bad wrapper-redaction-has-no-word-boundary"
  # EVERY other mention is a per-site escape — the definition line is the one exception.
  _rd_out=$(grep -n 'ROBOREV_MARKER_REDACTION' "$WRAPPER_REAL" \
    | awk -F: -v s="$_rl_start" -v e="$_rl_end" '$1 < s || $1 > e' \
    | grep -vE '^[0-9]+:ROBOREV_MARKER_REDACTION=' || true)
  [ -z "$_rd_out" ] || _rd_bad="$_rd_bad per-site-redaction-in-the-wrapper:${_rd_out%%$'\n'*}"
fi
grep -qF 'return MARKER_KEYWORD.sub(MARKER_KEYWORD_REDACTION, "".join(out))' "$SCAN_TOOL" \
  || _rd_bad="$_rd_bad scanner-emit-boundary-does-not-redact"
grep -qF '(?![A-Za-z])' "$SCAN_TOOL" || _rd_bad="$_rd_bad scanner-redaction-has-no-word-boundary"
# ONE SPELLING OF THE KEYWORD LIST: `judge_reason` must ASK the shared pattern, never carry its own
# copy, or the class gets fixed in one half again (which is what job 230 was).
grep -qF 'if MARKER_KEYWORD.search(reason):' "$SCAN_TOOL" \
  || _rd_bad="$_rd_bad reason-refusal-has-its-own-keyword-list"
if [ -z "$_rd_bad" ]; then
  ok 'structural: the keyword denylist is at safe_value + roborev_safe_line, word-bounded, with one spelling of the list'
else
  bad "structural: the authorization-keyword denylist is not a single boundary —$_rd_bad. A login, a gh diagnostic or any future value would carry a marker keyword into a block that gets pasted into PR comments (#3312 job 23 / roborev job 230)"
fi

printf '== structural: NOTICE is OUTSIDE the failing-capable verdict scan ==\n'
# `vacuity-tier1:` emits NOTICE as its documented ADVISORY value, so a scan that treated
# NOTICE* as failing would red every findings-bearing review on an advisory. Asserted against
# the SCAN ITSELF, not just against a case's observed exit code, so a future edit that adds
# NOTICE* to the failing set is caught here rather than by whichever case happens to exercise
# it.
#
# EVERY assert below is anchored to the SCAN STATEMENT, never to a file-wide grep. That
# distinction is the whole point and it is not pedantry: a file-wide `grep '"$SOME_KEY"'` is
# satisfied by the `emit_kv '<key>' "$SOME_KEY"` inside `emit_summary()`, so it would keep
# passing with the key DELETED from the scan — i.e. the assert meant to forbid the
# decorative-key defect would itself be decorative. Nor can a behavioural case always cover
# it: a key whose every failing assignment is (correctly) followed immediately by
# `finish FAIL 1` is never observed failing BY the scan, so its registration there is purely
# defensive — only a structural assert can pin it (#3229).
#
# So the scan is extracted ONCE, as a statement, and the asserts read that extract:
#   _scan_block = the whole `for verdict in … done` loop (bounds the `case` to INSIDE it)
#   _scan_keys  = the `for … ; do` KEY LIST alone (continuation-aware; this is what must
#                 name every per-check key)
#   _scan_case  = the classifying `case` line from within the loop
# ANCHORED ON `for scan_keyed in` (#3626): the scan carries each key's NAME beside its value,
# because one arm of its grammar is key-scoped (`DEFERRED`, admitted for `findings` alone). The
# needle is distinct from the affirmation backstop's `for keyed in "push-assert=` so the two
# extractions cannot pick each other up.
_scan_start=$(grep -nE '^[[:space:]]*for scan_keyed in ' "$WRAPPER_REAL" | head -1 | cut -d: -f1 || printf '')
_scan_end=''
if [ -n "$_scan_start" ]; then
  _scan_end=$(awk -v s="$_scan_start" 'NR>s && /^[[:space:]]*done[[:space:]]*$/ {print NR; exit}' "$WRAPPER_REAL")
fi
_scan_block=''
_scan_keys=''
_scan_case=''
if [ -n "$_scan_start" ] && [ -n "$_scan_end" ]; then
  _scan_block=$(sed -n "${_scan_start},${_scan_end}p" "$WRAPPER_REAL")
  # The key list ends at the first line that does not continue with a trailing backslash.
  _scan_keys=$(printf '%s\n' "$_scan_block" | awk '{print} !/\\$/{exit}')
  _scan_case=$(printf '%s\n' "$_scan_block" | grep -E 'case "\$verdict_token" in' | head -1 || printf '')
fi
if [ -z "$_scan_block" ] || [ -z "$_scan_keys" ] || [ -z "$_scan_case" ]; then
  bad 'structural: could not locate the wrapper verdict scan STATEMENT (for scan_keyed in … case … done) to inspect'
else
  ok "structural: the verdict scan is a single case over the per-check keys (lines $_scan_start-$_scan_end)"
  if printf '%s\n' "$_scan_keys" | grep -qE '; do[[:space:]]*$'; then
    ok 'structural: the extracted key list is the complete for-statement (terminates at "; do")'
  else
    bad 'structural: the extracted verdict-scan key list does not terminate at "; do" — the extraction is truncated, so the per-key asserts below would be unreliable'
  fi
  if printf '%s\n' "$_scan_case" | grep -qE 'case "\$verdict_token" in FAIL\|FINDINGS\|ERROR\|INCONSISTENT\)'; then
    ok 'structural: the failing-capable set is exactly FAIL|FINDINGS|ERROR|INCONSISTENT'
  else
    bad 'structural: the failing-capable verdict set is not the expected FAIL|FINDINGS|ERROR|INCONSISTENT'
  fi
  if printf '%s\n' "$_scan_case" | grep -q 'NOTICE'; then
    bad 'structural: NOTICE appears in the failing-capable verdict scan — an advisory vacuity-tier1 NOTICE would red RESULT:'
  else
    ok 'structural: NOTICE is absent from the failing-capable verdict scan'
  fi
  # ===== MATCHED ON THE VERDICT TOKEN, EXACTLY — the closure must not itself be a prefix
  # test (#3229 round-11 M3). A `PASS*` glob accepts `PASSthisNeverRan`, so the scan meant to
  # reject unplanned values would accept any value merely BEGINNING with a planned token.
  # Pinned structurally as well as behaviourally (cases cx28b/cx28c) because a future edit
  # could restore the globs while every behavioural case but those two stayed green.
  if printf '%s\n' "$_scan_block" | grep -qE '^[[:space:]]*verdict_token="\$\{verdict%% \*\}"'; then
    ok 'structural: the scan reduces each value to its VERDICT TOKEN (up to the first space) before classifying'
  else
    bad 'structural: the verdict scan does not extract a verdict token — it is classifying the whole value, so a token followed by anything is matched by prefix (#3229 M3)'
  fi
  if printf '%s\n' "$_scan_block" | grep -qE '(PASS|FAIL|SKIP|NOTICE|UNAVAILABLE|DEGRADED|NONE|PRESENT|UNKNOWN)\*'; then
    bad 'structural: a PREFIX GLOB (TOKEN*) survives in the verdict scan — PASSthisNeverRan would match PASS* and inherit the non-failing branch (#3229 M3)'
  else
    ok 'structural: the verdict scan carries NO prefix globs — every token is matched exactly'
  fi
  # ===== THE CLOSED-GRAMMAR INVARIANT, asserted STRUCTURALLY (#3229 round-10 sweep) =====
  # Case (cx28) proves ONE unrecognised value FAILs. Only a structural assert can pin that the
  # scan is keyed POSITIVELY AT ALL — i.e. that its non-failing branch is an ALLOW-LIST with a
  # failing `*)` fallback, rather than "everything that is not one of the four bad prefixes".
  # That distinction is the general form of three separate defects on this issue, and a future
  # edit could delete the positive arm while every behavioural case except cx28 stayed green.
  # Both halves are required: an allow-list whose fallthrough is permissive pins nothing.
  _scan_positive=$(printf '%s\n' "$_scan_block" \
    | grep -E 'PASS\|WAIVED\|SKIP\|NOTICE\|UNAVAILABLE\|DEGRADED' | head -1 || printf '')
  if [ -n "$_scan_positive" ]; then
    ok 'structural: the verdict scan has a POSITIVE arm — the non-failing set is an allow-list, not "not-failing"'
  else
    bad 'structural: the verdict scan has NO positive arm, so any value outside FAIL*|FINDINGS*|ERROR*|INCONSISTENT* — an empty string, a state a future check invents — inherits the non-failing branch and reaches finish PASS (#3229 round-10)'
  fi
  # The `*)` fallback must SET failed=1. Read from the positive `case`'s own body: from the
  # allow-list line to the `esac` that closes it.
  _scan_fallthrough=$(printf '%s\n' "$_scan_block" \
    | awk '/PASS\|WAIVED\|SKIP\|NOTICE/ { inb = 1 } inb { print } inb && /esac/ { exit }' \
    | grep -A 3 -E '^[[:space:]]*\*\)' || printf '')
  if printf '%s\n' "$_scan_fallthrough" | grep -qE '^[[:space:]]*failed=1[[:space:]]*$'; then
    ok 'structural: the positive arm FAILS CLOSED on an unrecognised value (its *) sets failed=1)'
  else
    bad 'structural: the verdict scan positive arm does not fail closed — an unrecognised value would be accepted silently, which is the shape this sweep closed'
  fi
  # THE AFFIRMATION BACKSTOP: a PASS requires every VERDICT-CARRYING key to be affirmatively
  # PASS — with NO exception, and no exemption mechanism. (One key was briefly exempt on a
  # `NOTICE`; both it and its exemption are gone — #3283/#3278 — which is STRICTER.) Case
  # (cx29) proves one un-run check is caught; this pins that the backstop exists and names all
  # six deterministic keys, so a key added to the block later is not silently exempt from it.
  # The `PASS)` arm is EXACT, matched on the token: a `PASS*` glob would let `PASSthisNeverRan`
  # satisfy the very backstop that exists to reject non-measurements (#3229 M3).
  if grep -qE '^[[:space:]]*not_affirmed="\$\{not_affirmed' "$WRAPPER_REAL" &&
    grep -qE '^[[:space:]]*PASS\) continue ;;' "$WRAPPER_REAL" &&
    grep -qE '^[[:space:]]*case "\$\{det_value%% \*\}" in' "$WRAPPER_REAL"; then
    ok 'structural: a PASS requires each verdict-carrying key to be affirmatively PASS, matched on the exact token (the SKIP backstop)'
    _aff_missing=""
    for _aff_key in push-assert census-check code-free sha-assert \
      review-completed prompt-content; do
      grep -qE "\"$_aff_key=\\\$[A-Z_]+\"" "$WRAPPER_REAL" || _aff_missing="$_aff_missing $_aff_key"
    done
    if [ -z "$_aff_missing" ]; then
      ok 'structural: all six deterministic keys are named in the affirmation backstop'
    else
      bad "structural: the affirmation backstop does not cover:$_aff_missing — those keys could ride to PASS on a non-measurement (SKIP)"
    fi
  else
    bad 'structural: the affirmation backstop is gone — a verdict-carrying key left at its initial SKIP (a check that never ran) would reach finish PASS, which is the vacuous pass this wrapper exists to prevent (#3229 round-10)'
  fi
  # AND IT CARRIES NO PER-KEY EXEMPTION. The backstop's body must contain exactly ONE
  # `continue` arm — the affirmative `PASS)` one. A deleted subsystem needed a second arm
  # exempting one key on `NOTICE`; with that subject gone the arm is
  # gone too, and this pins that no per-key escape hatch is reintroduced (an exempted key can
  # reach `finish PASS` on a non-measurement, which is the whole defect class). Read from the
  # backstop's own `case` body, so a `NOTICE*)` arm elsewhere in the wrapper cannot satisfy or
  # break it.
  # EXTRACTED TO A FILE and matched AS A FILE (#3387): every member of the chain below would
  # otherwise be `printf … | grep -q`, whose status this file's `pipefail` takes from a SIGPIPE-141
  # on a successful match — fail-open for the NEGATED members, which are the ones that forbid the
  # escape hatch. Same remedy as the `_dfs_opts` extraction documented above.
  _aff_body_f="$tmp/aff-body.txt"
  awk '/for keyed in "push-assert=/ { inb = 1 } inb { print } inb && /^[[:space:]]*esac[[:space:]]*$/ { exit }' \
    "$WRAPPER_REAL" >"$_aff_body_f"
  # AND THE NEGATED MEMBERS READ EXECUTABLE LINES ONLY. The comment block inside this `case`
  # RECORDS the deleted `DEFERRED` arm by name and says why it may not come back — the durable
  # artifact of the ruling — so scanning prose would make writing the history down a violation.
  # That is the same mistake as the job-18 census assert, and this suite has ruled on it twice.
  _aff_exec="$tmp/aff-body-exec.txt"
  grep -vE '^[[:space:]]*#' "$_aff_body_f" >"$_aff_exec" || true
  _aff_body=$(cat "$_aff_body_f")
  if [ -z "$_aff_body" ]; then
    bad 'structural: could not locate the affirmation backstop case body to inspect for per-key exemptions'
  else
    # EXACTLY ONE NON-`PASS` ADMISSION IS AUTHORISED, and it is the human-authorized absence waiver
    # (owner ruling (4), #3312). It is NOT the retired per-key hatch: that one admitted a `NOTICE` for a
    # named key in a machine-inferred mode, and both the mode and the inference are deleted. This one
    # admits `WAIVED` only when the provenance is COMPLETE — a granted state, an authorizer, a reason,
    # and a sha equal to the certified head — and is deliberately NOT keyed on `det_key`, so it cannot
    # become the "which keys are exempt" argument again. A third `continue`, or a provenance-free
    # admission, is the escape hatch #3229 forbade.
    # EXACTLY ONE NON-`PASS` ADMISSION IS AUTHORISED HERE: the #3312 absence WAIVER, gated on a
    # COMPLETE provenance and not keyed on `det_key` — because a waiver authorizes a PROPERTY (an
    # absence) that only one of these keys can ever report, so the provenance IS the whole test.
    #
    # AND THE #3626 FINDINGS DEFERRAL IS NOT ADMITTED HERE AT ALL — this assert is the INVERSE of the
    # one it replaces (roborev job 225). The replaced assert required a `DEFERRED)` arm reading
    # `deferral_admits` and required it NOT to be keyed on a key, i.e. it PINNED AS A PROPERTY the very
    # thing the spec forbids: "the deferral SHALL NOT be readable by, or applicable to, any check other
    # than the wrapper's `findings:` key". An assert that pins a spec violation is worse than no
    # assert, because it converts the defect into something a later fix has to fight. A deferral
    # authorizes a NAMED SET OF FINDINGS and says nothing about whether the reviewer's diff arrived or
    # the reviewed range matched, so admitting it for these six keys let ONE authorization excuse a
    # check nobody authorized. It is now confined by KEY NAME in the grammar scan (asserted just
    # below), and this loop must carry no `DEFERRED` arm and no read of the state. A THIRD `continue`,
    # a provenance-free admission, or any reappearance of `DEFERRED`/`deferral_admits` here, is the
    # escape hatch #3229 forbade.
    # Portable word boundaries, not GNU `\b` (#3453): POSIX ERE leaves `\b` UNDEFINED and BSD
    # grep on macOS — a first-class gate host — does not honour it, so this count would come
    # back 0 there and this affirmation check would silently measure NOTHING. Equivalence
    # measured against the GNU form (both count 3 on the same input).
    _aff_continues=$(grep -cE '(^|[^[:alnum:]_])continue([^[:alnum:]_]|$)' "$_aff_body_f" || true)
    if [ "$_aff_continues" -eq 2 ] &&
      grep -qE '^[[:space:]]*PASS\) continue ;;' "$_aff_body_f" &&
      grep -qF '"${ROBOREV_WAIVER_STATE:-}" = "granted"' "$_aff_body_f" &&
      grep -qF '"${ROBOREV_WAIVER_SCOPE:-}" = "base=${RANGE_BASE_SHA:-} head=${HEAD_SHA:-} job=${JOB:-}"' "$_aff_body_f" &&
      ! grep -qE '^[[:space:]]*DEFERRED\)' "$_aff_exec" &&
      ! grep -qF 'deferral_admits' "$_aff_exec" &&
      ! grep -qF 'det_key" = "prompt-content"' "$_aff_exec" &&
      ! grep -qF 'det_key" = "findings"' "$_aff_exec"; then
      ok 'structural (#3626): the affirmation backstop has the affirmative PASS arm plus exactly the WAIVED admission, gated on a complete provenance, and admits no DEFERRED and reads no deferral state — the findings deferral is confined to the findings key elsewhere'
    else
      bad "structural: the affirmation backstop carries $_aff_continues exempting arm(s), admits WAIVED without complete provenance, admits a DEFERRED (which would let a findings deferral excuse a check nobody authorized — spec: a deferral SHALL NOT be applicable to any check other than findings:), or is keyed on det_key (#3312/#3626 job 225)"
    fi
    # ===== AND THE CONFINEMENT IS ASSERTED WHERE IT LIVES: THE GRAMMAR SCAN, BY KEY NAME (#3626) =====
    # Both halves, because either alone is satisfiable by a scan that confines nothing: the scan must
    # CARRY the key name beside each value (a scan over bare values cannot express the confinement at
    # all), and its `DEFERRED` arm must REFUSE every key but `findings`. Read from the scan statement
    # extracted above, so a `scan_key` mentioned elsewhere in the wrapper can neither satisfy nor
    # break it.
    # THE EXTRACTS ARE WRITTEN TO FILES AND MATCHED AS FILES (#3387) — see the `_aff_body_f` note
    # above. Every member here is a POSITIVE requirement, so a SIGPIPE-141 would false-RED rather
    # than false-GREEN, but the shape is the one this suite has already measured inverting and the
    # remedy costs two lines.
    _dfk_bad=""
    _dfk_keys="$tmp/dfk-scan-keys.txt"
    _dfk_block="$tmp/dfk-scan-block.txt"
    printf '%s\n' "$_scan_keys" >"$_dfk_keys"
    printf '%s\n' "$_scan_block" >"$_dfk_block"
    grep -qF 'findings=$FINDINGS' "$_dfk_keys" || _dfk_bad="$_dfk_bad scan-does-not-carry-key-names"
    grep -qF 'scan_key="${scan_keyed%%=*}"' "$_dfk_block" || _dfk_bad="$_dfk_bad scan-key-not-split-from-the-pair"
    grep -qF 'if [ "$scan_key" != findings ]; then' "$_dfk_block" \
      || _dfk_bad="$_dfk_bad deferred-arm-not-confined-to-findings"
    if [ -z "$_dfk_bad" ]; then
      ok 'structural (#3626): the verdict scan carries each key NAME and admits DEFERRED for findings ALONE — a deferral cannot be read by, or applied to, any other check'
    else
      bad "structural (#3626): the DEFERRED admission is not confined to the findings key —$_dfk_bad. One authorization would then excuse a check nobody authorized, and the only thing preventing it would be that no other key HAPPENS to emit the token — #3564's latent-false-pass shape (roborev job 225)"
    fi
  fi
  # And the wrapper must STATE the rule, not just implement it: the next key added to this
  # block is written by someone reading the doc block, not the scan.
  if grep -qF 'A POSITIVE VERDICT REQUIRES AN AFFIRMATIVE MEASUREMENT' "$WRAPPER_REAL"; then
    ok 'structural: the wrapper states the affirmative-measurement rule the whole block obeys'
  else
    bad 'structural: the wrapper no longer states the affirmative-measurement rule — the invariant would have to be re-derived from the code by every reader'
  fi
  # THE REMOVAL, PINNED AT THE SCAN (#3229 owner ruling / #3283). The census-exclusion oracle
  # is deleted, so its key must be absent from the scan's key list AND from the block. Asserted
  # rather than assumed: a leftover `"$CENSUS_EXCLUSION"` in the key list would be a permanently
  # EMPTY value, which the closed grammar (correctly) FAILs — i.e. the residue of an incomplete
  # deletion would red every run, and it must be caught here rather than in the field.
  if printf '%s\n' "$_scan_keys" | grep -qE 'CENSUS_EXCLUSION'; then
    bad 'structural: the deleted CENSUS_EXCLUSION key is still named in the verdict-scan key list — it would hold a permanently empty value and red every run'
  else
    ok 'structural: the deleted census-exclusion key is absent from the verdict-scan key list'
  fi
  if grep -qE "emit_kv 'census-exclusion'" "$WRAPPER_REAL"; then
    bad 'structural: the summary block still emits a census-exclusion key whose oracle is deleted'
  else
    ok 'structural: the summary block no longer emits census-exclusion (removal visible in the OUTPUT contract, not just the source)'
  fi
  for _gone_fn in roborev_check_census_exclusion roborev_format_exclude_args \
    roborev_toml_exclude_patterns roborev_parse_toml_array roborev_corroborate_exclude_patterns; do
    if grep -qE "(^|[^#[:alnum:]_])$_gone_fn\b" "$WRAPPER_REAL" "$SCRIPT_DIR/../flow/roborev-review-oracles.sh" \
      "$SCRIPT_DIR/../flow/roborev-review-checks.sh"; then
      bad "structural: $_gone_fn is still referenced by the flow scripts — the deletion is incomplete"
    else
      ok "structural: $_gone_fn has no live reference in the flow scripts"
    fi
  done
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
# resolves exclude_patterns from the repo ROOT path and SNAPSHOTS them at daemon start, so
# until this change merges such a file is excluded from every review of it — `prompt-content:`
# would FAIL permanently, a deadlock rather than a test. Asserted structurally so it cannot
# be reintroduced.
_probe_exec_count=$(find "$SCRIPT_DIR/../../docs/reports/3229-artifacts" -maxdepth 1 \
  \( -name '*.sh' -o -name '*.py' -o -name '*.bt' \) 2>/dev/null | wc -l | tr -d '[:space:]')
if [ "${_probe_exec_count:-0}" -eq 0 ]; then
  ok 'structural: the #3229 artifacts dir carries NO executable (a pre-merge self-demonstration is a deadlock)'
else
  bad "structural: $_probe_exec_count executable(s) under docs/reports/3229-artifacts/ — an executable under root docs/ is dropped from its own review until this change merges, so prompt-content: FAILs permanently"
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
if grep -qE '^\s*roborev (review|show|list)' "$WRAPPER_REAL"; then
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
