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
WRAPPER="$SCRIPT_DIR/../flow/roborev-review.sh"
# The two sourced halves, named ONCE here because several cases probe a function DIRECTLY rather
# than through the wrapper. `WRAPPER` is reassigned later by the gate-mock cases; these are not.
CHECKS_SRC="$SCRIPT_DIR/../flow/roborev-review-checks.sh"
ORACLES_SRC="$SCRIPT_DIR/../flow/roborev-review-oracles.sh"
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
    # THE REAL SNAPSHOT LIFECYCLE (#3312), reproduced: roborev writes the snapshot diff, reviews it,
    # then DELETES the directory BEFORE `--wait` returns — measured live on job 3, which is why the
    # wrapper has to capture it while the review runs. Synchronisation is DETERMINISTIC, never a
    # sleep: the stub waits for the wrapper's capture to appear (bounded), then removes the snapshot.
    # A capture that never appears therefore FAILs the case, which is the point of it.
    if [ -n "${STUB_SNAPSHOT_LIFECYCLE_DIR:-}" ]; then
      mkdir -p "$STUB_SNAPSHOT_LIFECYCLE_DIR"
      snap_write() { # snap_write <file> <path>...
        local out="$1" q
        shift
        : >"$out"
        for q in "$@"; do
          printf 'diff --git a/%s b/%s\nindex 1111111..2222222 100644\n--- a/%s\n+++ b/%s\n@@ -0,0 +1 @@\n+x\n' \
            "$q" "$q" "$q" "$q" >>"$out"
        done
      }
      # THE PUBLISHED UNIT IS `<capture-dir>/<id>/meta` — the COMPLETED artifact. The capture directory
      # is private and per-run (mktemp -d), so it is DISCOVERED rather than known, and the wait is on
      # the finished publication, never on the content file appearing: waiting on the earlier of two
      # events is flaky by construction (roborev job 7, finding 2).
      snap_capture_of() { # snap_capture_of <snapshot-id> -> prints the NEWEST published meta path
        # Publications are `pub.<id>.<zero-padded-seq>`, so the LAST match of the glob is the newest —
        # the same rule the resolver uses. Scoped to this process's capture parent (job 9, F3).
        local d found=""
        for d in "${CAPTURES_PARENT:?}"/roborev-snapshot-capture.*/pub."$1".*/meta; do
          [ -f "$d" ] && found="$d"
        done
        [ -n "$found" ] || return 1
        printf '%s' "$found"
        return 0
      }
      snap_await() { # snap_await <snapshot-id> [<digest-to-differ-from>] -> 0 when published
                     # (and, when a digest is given, when it has CHANGED)
        local waited=0 meta dg
        while [ "$waited" -lt 600 ]; do
          if meta=$(snap_capture_of "$1"); then
            if [ -z "${2:-}" ]; then return 0; fi
            dg=$(sed -n 's/^digest=//p' "$meta" | head -1)
            [ -n "$dg" ] && [ "$dg" != "$2" ] && return 0
          fi
          waited=$((waited + 1))
          sleep 0.1
        done
        return 1
      }
      if [ -n "${STUB_SNAPSHOT_SYMLINK_TARGET:-}" ]; then
        # THE TOCTOU ORDERING: the snapshot file is a SYMLINK to a file OUTSIDE the repo, and the whole
        # directory is deleted before the wrapper validates. `-f` and `cp` both follow symlinks, so a
        # watcher that copied first would hold outside content with no path left to inspect.
        ln -s "$STUB_SNAPSHOT_SYMLINK_TARGET" "$STUB_SNAPSHOT_LIFECYCLE_DIR/roborev-snapshot-content.diff"
      else
        snap_write "$STUB_SNAPSHOT_LIFECYCLE_DIR/roborev-snapshot-content.diff" ${STUB_SNAPSHOT_PATHS:-}
      fi
      snap_id="${STUB_SNAPSHOT_LIFECYCLE_DIR##*/}"
      # THE POSITIVE CONTROL, in the SAME run: a second, ordinary snapshot directory whose capture MUST
      # be published. Waiting on IT rather than on a fixed sleep is what makes "the symlinked one was
      # not captured" a measurement instead of a race — the watcher is PROVEN alive before the case
      # concludes anything from an absence.
      wait_id="$snap_id"
      if [ -n "${STUB_SNAPSHOT_CONTROL_DIR:-}" ]; then
        mkdir -p "$STUB_SNAPSHOT_CONTROL_DIR"
        snap_write "$STUB_SNAPSHOT_CONTROL_DIR/roborev-snapshot-content.diff" control.rs
        wait_id="${STUB_SNAPSHOT_CONTROL_DIR##*/}"
      fi
      if [ -n "${STUB_SNAPSHOT_EXPECT_NO_CAPTURE:-}" ]; then
        # NO CAPTURE IS EXPECTED, so the wait is on evidence the watcher TRIED — the interposed copy's
        # own witness — plus one further poll interval. That makes the absence below a measurement
        # instead of a timeout, the same discipline as the (cx31s) control.
        waited=0
        while [ "$waited" -lt 600 ]; do
          [ -s "${CP_SHIM_WITNESS:-/nonexistent}" ] && break
          waited=$((waited + 1))
          sleep 0.1
        done
        sleep 1.5
        if snap_capture_of "$wait_id" >/dev/null; then
          printf '%s' "$(snap_capture_of "$wait_id")" >"$STUB_INVOKED.capture-seen"
        fi
      elif snap_await "$wait_id"; then
        printf '%s' "$(snap_capture_of "$wait_id")" >"$STUB_INVOKED.capture-seen"
      fi
      # SAME-LENGTH REWRITE (roborev job 7, finding 3): republish the snapshot with the SAME byte count
      # and DIFFERENT content, then wait for the digest recorded in `meta` to CHANGE. Reuse keyed on the
      # byte count would keep the stale copy and certify a diff other than the delivered one.
      if [ -n "${STUB_SNAPSHOT_REWRITE_PATHS:-}" ]; then
        before_dg=$(sed -n 's/^digest=//p' "$(snap_capture_of "$snap_id")" 2>/dev/null | head -1)
        before_len=$(wc -c <"$STUB_SNAPSHOT_LIFECYCLE_DIR/roborev-snapshot-content.diff" | tr -d '[:space:]')
        snap_write "$STUB_SNAPSHOT_LIFECYCLE_DIR/roborev-snapshot-content.diff" ${STUB_SNAPSHOT_REWRITE_PATHS}
        after_len=$(wc -c <"$STUB_SNAPSHOT_LIFECYCLE_DIR/roborev-snapshot-content.diff" | tr -d '[:space:]')
        printf '%s %s' "$before_len" "$after_len" >"$STUB_INVOKED.rewrite-lengths"
        snap_await "$snap_id" "$before_dg" && printf 'recaptured' >"$STUB_INVOKED.recaptured"
      fi
      if [ -n "${STUB_SNAPSHOT_SYMLINK_TARGET:-}" ] && snap_capture_of "$snap_id" >/dev/null; then
        printf 'captured' >"$STUB_INVOKED.symlink-captured"
      fi
      # REPLACE the snapshot file with a NON-REGULAR object instead of deleting the directory (job 10,
      # blocker 2): the live path then EXISTS but cannot be read as a diff, while a valid capture sits
      # ready. The block must say `unreadable`, not answer from the capture.
      if [ -n "${STUB_SNAPSHOT_REPLACE_WITH:-}" ]; then
        rm -f "$STUB_SNAPSHOT_LIFECYCLE_DIR/roborev-snapshot-content.diff"
        case "$STUB_SNAPSHOT_REPLACE_WITH" in
          dir)  mkdir -p "$STUB_SNAPSHOT_LIFECYCLE_DIR/roborev-snapshot-content.diff" ;;
          fifo) mkfifo "$STUB_SNAPSHOT_LIFECYCLE_DIR/roborev-snapshot-content.diff" 2>/dev/null \
                  || mkdir -p "$STUB_SNAPSHOT_LIFECYCLE_DIR/roborev-snapshot-content.diff" ;;
        esac
      else
        rm -rf "$STUB_SNAPSHOT_LIFECYCLE_DIR"
      fi
      rm -rf "${STUB_SNAPSHOT_CONTROL_DIR:-/nonexistent/x}"
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
  *) printf 'stub: unsupported roborev subcommand: %s\n' "$cmd" >&2; exit 64 ;;
esac
STUB
chmod +x "$stubbin/roborev"

# ---------------------------------------------------------------------------
# THE `cp` INTERPOSER (#3312, roborev job 8). The capture's copy step is OUR OWN code, so unlike the
# SIGTERM case there is no "unexpressible state" argument available: a source that changes while it is
# being copied is directly constructible. This shim stands in for `cp` ONLY for the snapshot content
# file (everything else is delegated to the real binary, since the wrapper copies other files too) and
# reproduces the two ways a staged copy can fail to be what was digested:
#   mutate-source   the copy succeeds and the SOURCE is then changed, which is what the loop's
#                   pre-copy/post-copy comparison exists to see. Deterministic by construction: no
#                   racing against a real concurrent writer.
#   truncate-copy   the copy "succeeds" but the DESTINATION holds fewer bytes than the source — the leg
#                   only the STAGED digest can catch, since the source is untouched and pre == post.
# Driven by CP_SHIM_MODE; each invocation appends to CP_SHIM_WITNESS, which is what the roborev stub
# waits on so "no capture was published" is a measurement rather than a timeout.
cpshim="$tmp/cpshim"
mkdir -p "$cpshim"
cat >"$cpshim/cp" <<'CPSHIM'
#!/usr/bin/env bash
set -uo pipefail
real=/bin/cp
src=""
for a in "$@"; do
  case "$a" in *roborev-snapshot-content.diff) src="$a" ;; esac
done
if [ -z "$src" ] || [ -z "${CP_SHIM_MODE:-}" ]; then exec "$real" "$@"; fi
dest="${@: -1}"
case "$CP_SHIM_MODE" in
  mutate-source)
    "$real" "$@" || exit $?
    printf 'diff --git a/mutated-during-copy.rs b/mutated-during-copy.rs\n' >>"$src"
    ;;
  truncate-copy)
    head -1 "$src" >"$dest" || exit $?
    ;;
  *) exec "$real" "$@" ;;
esac
printf '%s\n' "$CP_SHIM_MODE" >>"${CP_SHIM_WITNESS:-/dev/null}"
exit 0
CPSHIM
chmod +x "$cpshim/cp"
export CP_SHIM_MODE=''
export CP_SHIM_WITNESS=''
HAVE_REAL_CP=1
if [ ! -x /bin/cp ]; then
  HAVE_REAL_CP=0
  printf 'SKIP - /bin/cp is absent: the mid-copy-mutation cases cannot delegate to a real cp\n'
fi

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

# range_ref <work>: the git_ref shape a RANGE review records, "<base40>..<head40>".
range_ref() { printf '%s..%s' "$(git -C "$1" rev-parse "${2:-origin/main}")" "$(git -C "$1" rev-parse HEAD)"; }

# ===== THE CAPTURE PARENT IS PER TEST PROCESS (roborev job 9, F3) =====
# The capture directories are discovered by GLOB, and a glob over the shared temp dir can be satisfied
# by a CONCURRENT run's artifact — or by a leftover from a killed run that used the same hard-coded
# snapshot ids. A test that greens on evidence from a different run is the very false-PASS class this
# change exists to prevent, one layer down. So every wrapper run in this file gets `TMPDIR` pinned to a
# parent inside THIS process's `$tmp` (itself `mktemp -d`), and every discovery searches only there.
# Unique by construction, rather than coordinated around.
CAPTURES_PARENT="$tmp/captures"
mkdir -p "$CAPTURES_PARENT"
export CAPTURES_PARENT

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
  # HOME is redirected to a throwaway directory: nothing in the wrapper reads a roborev
  # config any more (#3283), but HERMETICITY is asserted structurally at the bottom of this
  # file and a host `$HOME/.roborev/` must never be able to influence a fixture run.
  STUB_INVOKED="$INVOKED" PATH="${WRAPPER_PATH_PREFIX:+$WRAPPER_PATH_PREFIX:}$stubbin:$PATH" HOME="$FIXTURE_HOME" \
    TMPDIR="${WRAPPER_TMPDIR:-$CAPTURES_PARENT}" \
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
export STUB_REVIEW_RC=0
export STUB_ANNOUNCE_SHA=''
# #3312: the snapshot LIFECYCLE knobs. STUB_SNAPSHOT_LIFECYCLE_DIR is the snapshot directory the
# stub creates and then deletes mid-review; STUB_SNAPSHOT_PATHS the paths it writes headers for;
# STUB_SNAPSHOT_CAPTURE_WAIT_DIR the wrapper's capture dir the stub waits on before deleting.
export STUB_SNAPSHOT_LIFECYCLE_DIR=''
export STUB_SNAPSHOT_PATHS=''
export STUB_SNAPSHOT_CAPTURE_WAIT_DIR=''
# STUB_SNAPSHOT_SYMLINK_TARGET makes the snapshot file a SYMLINK to that path (the TOCTOU fixture);
# STUB_SNAPSHOT_CONTROL_DIR is a second, ordinary snapshot dir used as the in-run positive control
# the stub waits on, so an ABSENT capture is a measurement rather than a race.
export STUB_SNAPSHOT_SYMLINK_TARGET=''
export STUB_SNAPSHOT_CONTROL_DIR=''
# STUB_SNAPSHOT_REWRITE_PATHS republishes the snapshot with these paths at the SAME byte length, to
# exercise digest-keyed reuse (a byte count cannot see a same-length rewrite).
export STUB_SNAPSHOT_REWRITE_PATHS=''
# STUB_SNAPSHOT_EXPECT_NO_CAPTURE flips the lifecycle wait from "wait for the publication" to "wait for
# the interposed copy's witness, then one more poll" — for the cases where a capture must be DISCARDED.
export STUB_SNAPSHOT_EXPECT_NO_CAPTURE=''
# STUB_SNAPSHOT_REPLACE_WITH leaves the snapshot DIRECTORY in place and replaces the content file with a
# non-regular object (`dir` or `fifo`), so the live path EXISTS but is unreadable while a capture exists.
export STUB_SNAPSHOT_REPLACE_WITH=''
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
  STUB_SNAPSHOT_LIFECYCLE_DIR=''
  STUB_SNAPSHOT_PATHS=''
  STUB_SNAPSHOT_CAPTURE_WAIT_DIR=''
  STUB_SNAPSHOT_SYMLINK_TARGET=''
  STUB_SNAPSHOT_CONTROL_DIR=''
  STUB_SNAPSHOT_REWRITE_PATHS=''
  STUB_SNAPSHOT_EXPECT_NO_CAPTURE=''
  STUB_SNAPSHOT_REPLACE_WITH=''
  CP_SHIM_MODE=''
  CP_SHIM_WITNESS=''
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
BASE_SHA="$3"
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
    _mut_from='HEAD "${BASE_SHA:-}"'; _mut_to='HEAD'
    _mut_lost=base-only-exec; _mut_lost2=base-exec-head-plain; _mut_kept=head-only-exec
  else
    _mut_from='HEAD "${BASE_SHA:-}"'; _mut_to='"${BASE_SHA:-}"'
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
# The per-endpoint predicate must stay RANGE-BLIND: if it learns about BASE_SHA it can express
# a precedence again, which is the ambiguity the split exists to remove.
cx3j_pred=$(awk '
  /^_roborev_mode_exec_state_at\(\) \{/ { inf = 1 }
  inf { print }
  inf && /^\}/ { exit }
' "$ORACLES_SRC")
if [ -z "$cx3j_pred" ]; then
  bad 'case (cx3j): _roborev_mode_exec_state_at was not found, so its range-blindness was not checked'
else
  if ! printf '%s\n' "$cx3j_pred" | grep -vE '^[[:space:]]*#' | grep -qE 'BASE_SHA|\bHEAD\b'; then
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
cp "$WRAPPER" "$SCRIPT_DIR/../flow/roborev-review-oracles.sh" \
  "$SCRIPT_DIR/../flow/roborev-review-checks.sh" "$_gm_dir/"
if [ -f "$SCRIPT_DIR/../flow/roborev-job-facts.py" ]; then
  cp "$SCRIPT_DIR/../flow/roborev-job-facts.py" "$_gm_dir/"
fi
work=$(make_fixture case_cx28 pushed)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
_gm_real_wrapper="$WRAPPER"
WRAPPER="$_gm_dir/roborev-review.sh"
run_wrapper "$work"
assert_verdict 'case (cx28 control) the UNPATCHED copy reaches PASS' PASS 0
assert_lacks 'case (cx28 control) and reports no grammar violation' 'verdict-grammar'
# ONE key, ONE value, outside the grammar. `MEASUREMENT-DID-NOT-HAPPEN` is deliberately not a
# near-miss of a recognised prefix, so the case pins the ALLOW-LIST rather than a spelling.
if sed_inplace_verified "$_gm_dir/roborev-review-checks.sh" \
  's/^    TIER1="PASS"$/    TIER1="MEASUREMENT-DID-NOT-HAPPEN"/' \
  '    TIER1="MEASUREMENT-DID-NOT-HAPPEN"' '    TIER1="PASS"'; then
  ok 'case (cx28): the unrecognised-verdict patch was really applied to the copy'
  run_wrapper "$work"
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
  run_wrapper "$work"
  assert_verdict 'case (cx29)' FAIL 1
  assert_says 'case (cx29) the un-run key is named as never having affirmatively passed' "^ERROR: verdict-affirmation: this run reached the PASS branch with a VERDICT-CARRYING key that never affirmatively passed — prompt-content: 'SKIP'\. "
  assert_says 'case (cx29) it states that a non-measurement is the vacuous pass itself' 'a non-failing value that is not a measurement'
  assert_says 'case (cx29) it points the reader at the wrapper, not the branch under review' 'NOT something to fix in the branch under review'
  assert_says 'case (cx29) the un-run key is visible in the block' '^prompt-content: SKIP$'
  assert_lacks 'case (cx29) and the grammar check does not misreport it as unrecognised' 'verdict-grammar'
fi
WRAPPER="$_gm_real_wrapper"

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
  WRAPPER="$_gm_dir/roborev-review.sh"
  # THE CONTROL, per case: the restored copy must reach PASS on this fixture, or a FAIL below
  # would prove nothing about the mutation.
  run_wrapper "$work"
  assert_verdict "case ($_np_label control) the restored copy reaches PASS" PASS 0
  if sed_inplace_verified "$_gm_dir/roborev-review-checks.sh" \
    "s/^    TIER1=\"PASS\"\$/    TIER1=\"$_np_value\"/" \
    "    TIER1=\"$_np_value\"" '    TIER1="PASS"'; then
    ok "case ($_np_label): the near-prefix patch was really applied to the copy"
    run_wrapper "$work"
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
  WRAPPER="$_gm_real_wrapper"
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
if grep -qF -- "review --branch --base origin/main --repo $work_canon --agent codex --model gpt-5.6-sol --wait" "$INVOKED"; then
  ok 'case (f): invoked over the census RANGE with an explicit absolute --repo + --wait'
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
# HOME is redirected for the same reason `run_wrapper` does it: a hand-rolled invocation
# must be as hermetic as the helper, so a host `$HOME/.roborev/` can never influence it.
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
# HOME redirected (see case t7): hermeticity, so nothing on the host can fail this case on
# the wrong key and leave the EXIT-trap assertion below unreached.
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

# =============================================================================================
# (cx31*) THE SNAPSHOT DIFF-DELIVERY MODE (issue #3312)
# =============================================================================================
# roborev has TWO diff-delivery modes, and the wrapper modelled only one. When the diff is
# large it is NOT inlined: the prompt ends with roborev's instruction to read it from a file,
# and carries ZERO `diff --git` headers. MEASURED on job 6836 (#3272, 23 files / +6561):
#
#     ### Combined Diff
#
#     (Diff too large to include inline)
#
#     The full diff has been written to a file for review.
#     Read the diff from: `<repo>/.roborev/roborev-snapshot-157393586/roborev-snapshot-content.diff`
#
#     Review the actual diff before writing findings.
#
# `prompt-content:` therefore reported `FAIL (21/21 code census paths absent from the prompt)`
# on a review that was genuine on every independent signal (1.47M input / 1.35M cached / 6.1k
# output, four findings with real file:line, both vacuity tiers PASS) — a FALSE FAIL on exactly
# the large diffs that most need review, and the documented way a guard gets waived.
#
# THE FIX IS NOT "AN EMPTY PROMPT IS A PASS" — it is "follow the diff to where it actually is".
# So the census, the canonical matcher and the fail-closed semantics are unchanged; only the
# SOURCE of the headers moves. Every new edge fails CLOSED with its OWN cause text, and each
# one below is OBSERVED firing (a guard written but never observed failing is not evidence).
#
# The path is taken ONLY from the prompt of the job whose record was already verified, and it
# is then bound to the reviewed repo + the snapshot shape, so a stale snapshot from another
# job or another checkout can never satisfy the check.
printf '== (cx31) snapshot diff-delivery mode: prompt names the diff by PATH ==\n'
# THE MEASURED PROMPT SHAPE, reproduced verbatim in structure. Written with `\\n` so the
# two-character escape survives into the JSON payload and is rendered by the stub's
# `printf %b`, exactly like PROMPT_WITH_PATHS.
snap_prompt() { # snap_prompt <abs-snapshot-path>...  -> the prompt roborev sends in snapshot mode
  local p
  printf 'Review the change on this branch.\\n\\n### Combined Diff\\n\\n(Diff too large to include inline)\\n\\nThe full diff has been written to a file for review.'
  for p in "$@"; do
    printf '\\nRead the diff from: `%s`' "$p"
  done
  printf '\\n\\nReview the actual diff before writing findings.'
}
# A snapshot file with the shape roborev writes: real `diff --git` headers plus body lines.
write_snap_diff() { # write_snap_diff <file> <path>...
  local f="$1" p
  shift
  mkdir -p "$(dirname "$f")"
  : >"$f"
  for p in "$@"; do
    printf 'diff --git a/%s b/%s\nindex 1111111..2222222 100644\n--- a/%s\n+++ b/%s\n@@ -0,0 +1 @@\n+x\n' \
      "$p" "$p" "$p" "$p" >>"$f"
  done
}
# ONE fixture for the whole family: only the PROMPT and the snapshot files differ per case, so
# reusing it keeps each case a one-variable change. `two-code-commits` gives exactly TWO code
# census paths (alpha.rs, beta.rs), so a `2/2` count is discriminating and a `1/2` names which
# one was missing.
snap_work=$(make_fixture case_cx31 two-code-commits)
snap_repo=$(cd "$snap_work" && pwd -P)
snap_dir="$snap_repo/.roborev/roborev-snapshot-157393586"
snap_file="$snap_dir/roborev-snapshot-content.diff"

reset_stub
write_snap_diff "$snap_file" alpha.rs beta.rs
STUB_ANNOUNCE_SHA=$(git -C "$snap_work" rev-parse HEAD)
STUB_PROMPT=$(snap_prompt "$snap_file")
run_wrapper "$snap_work"
assert_verdict 'case (cx31)' PASS 0
assert_says 'case (cx31) the snapshot-delivered census is PASS, and the value says WHERE it was found' \
  '^prompt-content: PASS \(2/2 code census paths present in the snapshot diff\)$'
assert_lacks 'case (cx31) never the false FAIL this issue reports' '^prompt-content: FAIL'
assert_enqueued 'case (cx31)'
assert_one_result_line 'case (cx31)'

printf '== (cx31b) T3 PRESERVED: a snapshot MISSING a census code path still FAILs, and names it ==\n'
reset_stub
write_snap_diff "$snap_file" alpha.rs
STUB_ANNOUNCE_SHA=$(git -C "$snap_work" rev-parse HEAD)
STUB_PROMPT=$(snap_prompt "$snap_file")
run_wrapper "$snap_work"
assert_verdict 'case (cx31b)' FAIL 1
assert_says 'case (cx31b) the absent path is counted against the snapshot, not excused' \
  '^prompt-content: FAIL \(1/2 code census paths absent from the snapshot diff the prompt names\)$'
assert_says 'case (cx31b) the absent path is NAMED' '^  beta\.rs$'
assert_lacks 'case (cx31b) never a PASS on a snapshot missing a code path' '^prompt-content: PASS'

printf '== (cx31c) BOTH sources present: the header sets are UNIONED ==\n'
reset_stub
write_snap_diff "$snap_file" beta.rs
STUB_ANNOUNCE_SHA=$(git -C "$snap_work" rev-parse HEAD)
# An inline header for alpha.rs AND a snapshot carrying beta.rs: neither source alone covers
# the census, so a green here can only come from the union.
STUB_PROMPT="diff --git a/alpha.rs b/alpha.rs\\n$(snap_prompt "$snap_file")"
run_wrapper "$snap_work"
assert_verdict 'case (cx31c)' PASS 0
assert_says 'case (cx31c) the union of prompt + snapshot covers the census' \
  '^prompt-content: PASS \(2/2 code census paths present in the prompt and its snapshot diff\)$'

printf '== (cx31d) NO source at all: neither an inline diff nor a snapshot path ==\n'
reset_stub
STUB_ANNOUNCE_SHA=$(git -C "$snap_work" rev-parse HEAD)
STUB_PROMPT="$PROMPT_WITHOUT_PATHS"
run_wrapper "$snap_work"
assert_verdict 'case (cx31d)' FAIL 1
# TODAY'S BEHAVIOUR IS PRESERVED in the value (the paths really were not delivered), with the
# no-source condition named as its OWN cause so it is distinguishable from a snapshot fault.
assert_says 'case (cx31d) the value still counts the undelivered paths' \
  '^prompt-content: FAIL \(2/2 code census paths absent from the prompt\)$'
assert_says 'case (cx31d) the no-source condition is its own named cause' \
  "^ERROR: prompt-content: the prompt carries NEITHER an inline diff .* NOR a snapshot diff path"

printf '== (cx31e) the snapshot directory was already CLEANED UP (its own cause) ==\n'
reset_stub
write_snap_diff "$snap_file" alpha.rs beta.rs
STUB_ANNOUNCE_SHA=$(git -C "$snap_work" rev-parse HEAD)
STUB_PROMPT=$(snap_prompt "$snap_file")
rm -rf "$snap_dir"
run_wrapper "$snap_work"
assert_verdict 'case (cx31e)' FAIL 1
assert_says 'case (cx31e) the key names the cleaned-up snapshot' \
  '^prompt-content: FAIL \(snapshot diff unusable: cleaned-up\)$'
assert_says 'case (cx31e) the cause names the TRANSIENT directory and the path' \
  "^ERROR: prompt-content: .*snapshot DIRECTORY no longer exists.*$snap_dir"
assert_lacks 'case (cx31e) never a PASS on an unreadable oracle' '^prompt-content: PASS'

printf '== (cx31f) the snapshot directory exists but the named FILE does not ==\n'
reset_stub
mkdir -p "$snap_dir"
rm -f "$snap_file"
STUB_ANNOUNCE_SHA=$(git -C "$snap_work" rev-parse HEAD)
STUB_PROMPT=$(snap_prompt "$snap_file")
run_wrapper "$snap_work"
assert_verdict 'case (cx31f)' FAIL 1
assert_says 'case (cx31f) a missing file is distinct from a cleaned-up directory' \
  '^prompt-content: FAIL \(snapshot diff unusable: missing\)$'
assert_says 'case (cx31f) the cause names the path and that no capture stood in for it' \
  "^ERROR: prompt-content: .*snapshot directory EXISTS but the named diff file is not in it, and no capture of it was taken: $snap_file"

printf '== (cx31g) the named snapshot path is not a readable regular file ==\n'
reset_stub
rm -rf "$snap_file"
# A DIRECTORY at the named path: root-immune (a `chmod 000` file is still readable as root, so
# a permission-based fixture would silently stop testing this on a privileged runner).
mkdir -p "$snap_file"
STUB_ANNOUNCE_SHA=$(git -C "$snap_work" rev-parse HEAD)
STUB_PROMPT=$(snap_prompt "$snap_file")
run_wrapper "$snap_work"
assert_verdict 'case (cx31g)' FAIL 1
assert_says 'case (cx31g) a non-regular/unreadable snapshot is its own cause' \
  '^prompt-content: FAIL \(snapshot diff unusable: unreadable\)$'
assert_says 'case (cx31g) the cause names the path' \
  "^ERROR: prompt-content: .*not a READABLE REGULAR FILE: $snap_file"
rmdir "$snap_file"

printf '== (cx31h) the named snapshot file is EMPTY ==\n'
reset_stub
mkdir -p "$snap_dir"
printf '   \n\n' >"$snap_file"
STUB_ANNOUNCE_SHA=$(git -C "$snap_work" rev-parse HEAD)
STUB_PROMPT=$(snap_prompt "$snap_file")
run_wrapper "$snap_work"
assert_verdict 'case (cx31h)' FAIL 1
assert_says 'case (cx31h) an empty snapshot is its own cause' \
  '^prompt-content: FAIL \(snapshot diff unusable: empty\)$'
assert_says 'case (cx31h) the cause names the path' \
  "^ERROR: prompt-content: .*snapshot diff is EMPTY.*: $snap_file"

printf '== (cx31i) the snapshot file carries ZERO diff --git headers (a 0/0 is never a pass) ==\n'
reset_stub
printf 'this file exists and has content but carries no header at all\n' >"$snap_file"
STUB_ANNOUNCE_SHA=$(git -C "$snap_work" rev-parse HEAD)
STUB_PROMPT=$(snap_prompt "$snap_file")
run_wrapper "$snap_work"
assert_verdict 'case (cx31i)' FAIL 1
assert_says 'case (cx31i) a header-less snapshot is its own cause' \
  '^prompt-content: FAIL \(snapshot diff unusable: no-headers\)$'
assert_says 'case (cx31i) the cause names the path' \
  "^ERROR: prompt-content: .*carries NO .diff --git. header at all: $snap_file"

printf '== (cx31j) a snapshot OUTSIDE the reviewed repo cannot satisfy the check ==\n'
reset_stub
# The strongest form of this case: the foreign snapshot is PERFECTLY VALID and carries EVERY
# census code path. Only the repo binding stands between it and a green — so a PASS here would
# mean another checkout's (or another job's) diff certifying this review.
foreign_snap="$tmp/foreign-repo/.roborev/roborev-snapshot-999/roborev-snapshot-content.diff"
write_snap_diff "$foreign_snap" alpha.rs beta.rs
STUB_ANNOUNCE_SHA=$(git -C "$snap_work" rev-parse HEAD)
STUB_PROMPT=$(snap_prompt "$foreign_snap")
run_wrapper "$snap_work"
assert_verdict 'case (cx31j)' FAIL 1
assert_says 'case (cx31j) a foreign-repo snapshot FAILs' \
  '^prompt-content: FAIL \(snapshot diff unusable: foreign-repo\)$'
assert_says 'case (cx31j) the cause names both the path and the reviewed repo' \
  "^ERROR: prompt-content: .*OUTSIDE the reviewed repository: $foreign_snap is not under $snap_repo"
assert_lacks 'case (cx31j) a valid foreign snapshot never reaches a PASS' '^prompt-content: PASS'

printf '== (cx31k) a path inside the repo that is NOT a roborev snapshot for this review ==\n'
reset_stub
stale_snap="$snap_repo/.roborev/stale/roborev-snapshot-content.diff"
write_snap_diff "$stale_snap" alpha.rs beta.rs
STUB_ANNOUNCE_SHA=$(git -C "$snap_work" rev-parse HEAD)
STUB_PROMPT=$(snap_prompt "$stale_snap")
run_wrapper "$snap_work"
assert_verdict 'case (cx31k)' FAIL 1
assert_says 'case (cx31k) an unbound in-repo path FAILs' \
  '^prompt-content: FAIL \(snapshot diff unusable: unbound-job\)$'
assert_says 'case (cx31k) the cause names WHICH component is wrong, and the path' \
  "^ERROR: prompt-content: .*NOT a roborev snapshot for this review — the second component 'stale' is not a 'roborev-snapshot-<id>' directory\. Path: $stale_snap"

printf '== (cx31k2) a SYMLINK at the named path is refused, not followed ==\n'
# FOUND BY ROBOREV ON THIS CHANGE (job 5, Medium): the containment test is decided on the PATH, and
# the ancestor resolution canonicalises only the DIRECTORIES — so a symlink at the FINAL component
# can sit inside the repo and point anywhere on the host, which would let an arbitrary file stand in
# as this review's diff. The fixture is the strongest form: the symlink target is a VALID snapshot
# carrying every census path, so only the refusal stands between it and a green.
reset_stub
mkdir -p "$snap_dir"
rm -f "$snap_file"
write_snap_diff "$tmp/outside-target.diff" alpha.rs beta.rs
ln -s "$tmp/outside-target.diff" "$snap_file"
STUB_ANNOUNCE_SHA=$(git -C "$snap_work" rev-parse HEAD)
STUB_PROMPT=$(snap_prompt "$snap_file")
run_wrapper "$snap_work"
assert_verdict 'case (cx31k2)' FAIL 1
assert_says 'case (cx31k2) a symlinked snapshot path is its own cause' \
  '^prompt-content: FAIL \(snapshot diff unusable: symlinked\)$'
assert_says 'case (cx31k2) the cause names the path and why it is refused rather than followed' \
  "^ERROR: prompt-content: the snapshot path the prompt names cannot be trusted to name what it appears to name — the named file itself is a SYMLINK \('$snap_file'\)"
assert_lacks 'case (cx31k2) a valid out-of-repo target never reaches a PASS through a symlink' '^prompt-content: PASS'
rm -f "$snap_file"

printf '== (cx31l) the prompt names TWO DIFFERENT snapshot paths ==\n'
reset_stub
snap_second="$snap_repo/.roborev/roborev-snapshot-2222/roborev-snapshot-content.diff"
write_snap_diff "$snap_file" alpha.rs beta.rs
write_snap_diff "$snap_second" alpha.rs beta.rs
STUB_ANNOUNCE_SHA=$(git -C "$snap_work" rev-parse HEAD)
STUB_PROMPT=$(snap_prompt "$snap_file" "$snap_second")
run_wrapper "$snap_work"
assert_verdict 'case (cx31l)' FAIL 1
assert_says 'case (cx31l) an ambiguous snapshot set FAILs rather than picking one' \
  '^prompt-content: FAIL \(snapshot diff unusable: ambiguous\)$'
assert_says 'case (cx31l) the cause names how many were found' \
  '^ERROR: prompt-content: .*names 2 DIFFERENT snapshot diff paths'
rm -rf "$snap_repo/.roborev/roborev-snapshot-2222"

printf '== (cx31m) a RELATIVE snapshot path is refused, never resolved against $PWD ==\n'
reset_stub
write_snap_diff "$snap_file" alpha.rs beta.rs
STUB_ANNOUNCE_SHA=$(git -C "$snap_work" rev-parse HEAD)
STUB_PROMPT=$(snap_prompt '.roborev/roborev-snapshot-157393586/roborev-snapshot-content.diff')
run_wrapper "$snap_work"
assert_verdict 'case (cx31m)' FAIL 1
assert_says 'case (cx31m) a relative snapshot path is its own cause' \
  '^prompt-content: FAIL \(snapshot diff unusable: not-absolute\)$'
assert_says 'case (cx31m) the cause explains what it would resolve against' \
  "^ERROR: prompt-content: .*RELATIVE snapshot diff path"

printf '== (cx31n) the instruction line carries no readable path ==\n'
reset_stub
STUB_ANNOUNCE_SHA=$(git -C "$snap_work" rev-parse HEAD)
STUB_PROMPT='Review the change.\n\nThe full diff has been written to a file for review.\nRead the diff from: somewhere, probably\n\nReview the actual diff before writing findings.'
run_wrapper "$snap_work"
assert_verdict 'case (cx31n)' FAIL 1
assert_says 'case (cx31n) an unparseable instruction line is its own cause' \
  '^prompt-content: FAIL \(snapshot diff unusable: unparseable-path\)$'
assert_says 'case (cx31n) the cause says what could not be read' \
  '^ERROR: prompt-content: .*no backtick-delimited path could be read'

printf '== (cx31r) THE REAL LIFECYCLE: roborev deletes the snapshot before --wait returns ==\n'
# MEASURED LIVE, and it falsified the first design of this fix (job 3 on this fleet: 12 files, +3311, a
# 232,820-byte snapshot with 9 headers, 541,812 input / 472,576 cached tokens, a genuine completed
# review) — roborev removes `.roborev/roborev-snapshot-<id>/` when the review finishes, which is BEFORE
# `roborev review --wait` returns to the wrapper. Reading the path afterwards therefore reported
# `cleaned-up` on EVERY genuine snapshot-mode review: a differently-named FAIL is not a fix.
# `roborev show` cannot hand the diff back either (--prompt/--json, no --diff), so the wrapper captures
# the snapshot WHILE the review runs, into a PRIVATE per-run directory, and reads its own copy of the
# path the prompt names. The stub drives that whole path: it writes the snapshot, waits for the
# COMPLETED publication, DELETES the snapshot, and only then announces the job.
reset_stub
snap_live_dir="$snap_repo/.roborev/roborev-snapshot-424242"
snap_live_log="$tmp/cx31r-transcript.log"
STUB_SNAPSHOT_LIFECYCLE_DIR="$snap_live_dir"
STUB_SNAPSHOT_PATHS='alpha.rs beta.rs'
STUB_ANNOUNCE_SHA=$(git -C "$snap_work" rev-parse HEAD)
STUB_PROMPT=$(snap_prompt "$snap_live_dir/roborev-snapshot-content.diff")
run_wrapper "$snap_work" --log "$snap_live_log"
assert_verdict 'case (cx31r)' PASS 0
assert_says 'case (cx31r) the captured snapshot is what certifies the census, and the value says so' \
  '^prompt-content: PASS \(2/2 code census paths present in the captured snapshot diff\)$'
assert_lacks 'case (cx31r) the deleted snapshot is not reported as cleaned-up' 'snapshot diff unusable'
if [ -d "$snap_live_dir" ]; then
  bad 'case (cx31r): the stub did not delete the snapshot directory — the case did not reproduce the real lifecycle'
else
  ok 'case (cx31r): the snapshot directory was really gone by the time prompt-content ran'
fi
# The capture directory is private and removed at exit, so its presence is witnessed DURING the run by
# the stub (which waited on the completed publication) rather than inspected afterwards.
if [ -s "$INVOKED.capture-seen" ]; then
  ok 'case (cx31r): a COMPLETED capture (content + identity, one rename) was published while the review ran'
else
  bad 'case (cx31r): no completed capture was observed during the run, so the PASS above could not have come from one'
fi
# ...and it does not survive the run: a stable, reusable capture path is the staleness class the
# private directory removes by construction.
if snap_capture_dirs=$(ls -d "$CAPTURES_PARENT"/roborev-snapshot-capture.*/pub.roborev-snapshot-424242.* 2>/dev/null); then
  bad "case (cx31r): the capture outlived the wrapper ($snap_capture_dirs) — a reusable capture directory is exactly the reuse hazard the private one removes"
else
  ok 'case (cx31r): the private capture directory was removed at exit (nothing to go stale, nothing to reuse)'
fi
reset_stub

printf '== (cx31r2) with the capture MISSING, the same lifecycle is still a cleaned-up FAIL ==\n'
# The one-variable control for (cx31r): same deleted snapshot, no capture. Without it, a green
# (cx31r) could not be attributed to the capture — and this direction is the fail-closed one that
# must survive, so it is observed rather than reasoned about.
reset_stub
STUB_ANNOUNCE_SHA=$(git -C "$snap_work" rev-parse HEAD)
STUB_PROMPT=$(snap_prompt "$snap_repo/.roborev/roborev-snapshot-555555/roborev-snapshot-content.diff")
run_wrapper "$snap_work"
assert_verdict 'case (cx31r2)' FAIL 1
assert_says 'case (cx31r2) an uncaptured, deleted snapshot FAILs' \
  '^prompt-content: FAIL \(snapshot diff unusable: cleaned-up\)$'
assert_says 'case (cx31r2) the cause names the capture that would have been the evidence' \
  'is the only possible evidence and it is not there'

printf '== (cx31s) TOCTOU: a SYMLINKED snapshot source is never captured (roborev job 6, blocker 1) ==\n'
# THE ORDERING THAT DEFEATS VALIDATION-TIME CHECKING: `-f` and `cp` both FOLLOW symlinks, and roborev
# deletes the original BEFORE the wrapper validates — so a watcher that copied first would hold an
# OUTSIDE file as this review's diff with no path left to inspect. The refusal therefore has to happen
# at CAPTURE time. The fixture is the strongest form: the symlink target is a VALID snapshot carrying
# every census path, so only the capture-time refusal stands between it and a green.
reset_stub
write_snap_diff "$tmp/toctou-outside-target.diff" alpha.rs beta.rs
toctou_dir="$snap_repo/.roborev/roborev-snapshot-777777"
toctou_control="$snap_repo/.roborev/roborev-snapshot-777778"
toctou_log="$tmp/cx31s-transcript.log"
STUB_SNAPSHOT_LIFECYCLE_DIR="$toctou_dir"
STUB_SNAPSHOT_SYMLINK_TARGET="$tmp/toctou-outside-target.diff"
STUB_SNAPSHOT_CONTROL_DIR="$toctou_control"
STUB_ANNOUNCE_SHA=$(git -C "$snap_work" rev-parse HEAD)
STUB_PROMPT=$(snap_prompt "$toctou_dir/roborev-snapshot-content.diff")
run_wrapper "$snap_work" --log "$toctou_log"
assert_verdict 'case (cx31s)' FAIL 1
assert_lacks 'case (cx31s) an outside file never certifies the review through a symlinked capture' '^prompt-content: PASS'
# THE POSITIVE CONTROL FIRST: the ordinary sibling WAS captured, so the watcher was demonstrably alive
# and polling — without this, the absence below could just be a race.
if [ -s "$INVOKED.capture-seen" ]; then
  ok 'case (cx31s): the watcher was alive (the ordinary sibling snapshot WAS captured)'
else
  bad 'case (cx31s): the control snapshot was not captured, so nothing below can be concluded from an absent capture'
fi
if [ -f "$INVOKED.symlink-captured" ]; then
  bad 'case (cx31s): the SYMLINKED snapshot was captured — cp followed the symlink and an outside file became this review evidence (#3312 blocker 1)'
else
  ok 'case (cx31s): the symlinked snapshot was refused at capture time (never published)'
fi
assert_says 'case (cx31s) with nothing captured the verdict is the fail-closed cleaned-up cause' \
  '^prompt-content: FAIL \(snapshot diff unusable: cleaned-up\)$'
reset_stub

printf '== (cx31v) a SAME-LENGTH rewrite of the snapshot is re-captured, not reused (job 7, finding 3) ==\n'
# Reuse keyed on the byte count keeps a stale capture when the snapshot is rewritten to the SAME length,
# certifying a diff other than the one delivered. The rewrite swaps `beta.rs` for `zeta.rs` — the same
# number of bytes, a different census path — so the stale copy would report 2/2 while the delivered diff
# no longer carries `beta.rs` at all.
reset_stub
rewrite_dir="$snap_repo/.roborev/roborev-snapshot-848484"
rewrite_log="$tmp/cx31v-transcript.log"
STUB_SNAPSHOT_LIFECYCLE_DIR="$rewrite_dir"
STUB_SNAPSHOT_PATHS='alpha.rs beta.rs'
STUB_SNAPSHOT_REWRITE_PATHS='alpha.rs zeta.rs'
STUB_ANNOUNCE_SHA=$(git -C "$snap_work" rev-parse HEAD)
STUB_PROMPT=$(snap_prompt "$rewrite_dir/roborev-snapshot-content.diff")
run_wrapper "$snap_work" --log "$rewrite_log"
# FIXTURE INTEGRITY FIRST: the rewrite must really be the same length, or the case tests nothing.
_rw_lengths=$(cat "$INVOKED.rewrite-lengths" 2>/dev/null || printf '')
if [ -n "$_rw_lengths" ] && [ "${_rw_lengths% *}" = "${_rw_lengths#* }" ]; then
  ok "case (cx31v): the rewrite is byte-identical in LENGTH ($_rw_lengths) — a size-keyed reuse could not see it"
else
  bad "case (cx31v): the rewrite changed the byte count ('${_rw_lengths:-<unmeasured>}'), so this case does not exercise the same-length class"
fi
if [ -f "$INVOKED.recaptured" ]; then
  ok 'case (cx31v): the watcher re-captured on the digest change'
else
  bad 'case (cx31v): the digest change was never re-captured, so the stale copy is what prompt-content read'
fi
assert_verdict 'case (cx31v)' FAIL 1
assert_says 'case (cx31v) the re-captured diff no longer carries beta.rs, and that is reported' \
  '^prompt-content: FAIL \(1/2 code census paths absent from the snapshot diff captured while the review ran\)$'
assert_says 'case (cx31v) the absent path is NAMED' '^  beta\.rs$'
reset_stub

printf "== (cx31t) a capture with NO identity record is REJECTED (direct unit probe) ==\n"
# The consumer half: "which file did we copy" must be an AFFIRMATIVE recorded fact the reader REQUIRES,
# so a capture it cannot identify is non-passing rather than assumed to be the right one. Probed
# DIRECTLY, because the production path can no longer produce this state — publication is a single
# rename into a private per-run directory, so content without its identity record is unreachable
# through the wrapper. That is exactly why the check stays: it is a structural backstop whose whole
# value is not depending on the producer still being correct.
cx31t_probe="$tmp/cx31t-probe.sh"
cx31t_out="$tmp/cx31t-out.txt"
cat >"$cx31t_probe" <<'CX31T'
set -uo pipefail
REPO="$3"
. "$1"   # oracles
. "$2"   # checks
LOG="$4/cx31t.log"
PROMPT_FILE="$LOG.prompt"
ROBOREV_SNAPSHOT_CAPTURE_DIR="$4/capture"
# A capture with content but NO meta — the state the private-directory design makes unreachable.
mkdir -p "$ROBOREV_SNAPSHOT_CAPTURE_DIR/pub.roborev-snapshot-313131.000001"
printf 'diff --git a/alpha.rs b/alpha.rs\n' >"$ROBOREV_SNAPSHOT_CAPTURE_DIR/pub.roborev-snapshot-313131.000001/content.diff"
printf 'Read the diff from: `%s`\n' "$REPO/.roborev/roborev-snapshot-313131/roborev-snapshot-content.diff" >"$PROMPT_FILE"
CENSUS='1 file, +1/-0'
BASE='origin/main'
JOB=9
census_code_paths=(alpha.rs)
DETAILS=()
PROMPT_CONTENT=""
roborev_check_prompt_content
printf 'prompt-content: %s\n' "$PROMPT_CONTENT"
for d in ${DETAILS[@]+"${DETAILS[@]}"}; do printf '%s\n' "$d"; done
CX31T
mkdir -p "$tmp/cx31t-repo" && (cd "$tmp/cx31t-repo" && mkdir -p .roborev)
if bash "$cx31t_probe" "$ORACLES_SRC" "$CHECKS_SRC" "$tmp/cx31t-repo" "$tmp" >"$cx31t_out" 2>&1; then
  if grep -qE '^prompt-content: FAIL \(snapshot diff unusable: capture-unidentified\)$' "$cx31t_out"; then
    ok 'case (cx31t): a capture without its identity record is refused, under its own cause'
  else
    bad "case (cx31t): expected the capture-unidentified refusal, got '$(head -2 "$cx31t_out")'"
  fi
  if grep -qF 'a capture we cannot identify is not evidence of what the reviewer received' "$cx31t_out"; then
    ok 'case (cx31t): the cause says why an unidentifiable capture cannot pass'
  else
    bad 'case (cx31t): the cause does not explain the refusal'
  fi
  if grep -qE '^prompt-content: PASS' "$cx31t_out"; then
    bad 'case (cx31t): an unidentified capture reached a PASS'
  else
    ok 'case (cx31t): no PASS is reachable from an unidentified capture'
  fi
else
  bad "case (cx31t): the unit probe did not run: $(cat "$cx31t_out")"
fi

printf '== (cx31u) a capture is matched by its FULL path, not the snapshot dir id (blocker 2) ==\n'
# The prompt names a DIFFERENT file in the same snapshot directory. The canonical sibling's capture is
# present, complete and valid — so an id-keyed lookup would answer with it and certify a diff the
# reviewer was never instructed to read.
reset_stub
mism_dir="$snap_repo/.roborev/roborev-snapshot-616161"
mism_log="$tmp/cx31u-transcript.log"
STUB_SNAPSHOT_LIFECYCLE_DIR="$mism_dir"
STUB_SNAPSHOT_PATHS='alpha.rs beta.rs'
STUB_ANNOUNCE_SHA=$(git -C "$snap_work" rev-parse HEAD)
STUB_PROMPT=$(snap_prompt "$mism_dir/some-other-diff-the-reviewer-was-pointed-at.diff")
run_wrapper "$snap_work" --log "$mism_log"
assert_verdict 'case (cx31u)' FAIL 1
assert_says 'case (cx31u) the canonical sibling capture does NOT stand in for another file' \
  '^prompt-content: FAIL \(snapshot diff unusable: capture-path-mismatch\)$'
assert_says 'case (cx31u) the cause names both the recorded and the instructed path' \
  "records the captured path '\.roborev/roborev-snapshot-616161/roborev-snapshot-content\.diff', but the prompt instructed the reviewer to read '\.roborev/roborev-snapshot-616161/some-other-diff-the-reviewer-was-pointed-at\.diff'"
# The sibling capture really was published — otherwise the mismatch above would be vacuous.
if [ -s "$INVOKED.capture-seen" ]; then
  ok 'case (cx31u): the canonical capture existed and was still refused for the other path'
else
  bad 'case (cx31u): no capture existed, so the mismatch cause was reached without a substitute being available'
fi
reset_stub

printf '== (cx31w) a source mutated DURING the copy is DISCARDED, not published (job 8) ==\n'
# THE BLOCKER, constructed deterministically. The copy step is our own code, so a source that changes
# while it is read is directly expressible: the interposed `cp` copies and then appends to the SOURCE,
# which is exactly what the pre-copy/post-copy digest comparison exists to see. Before the fix the
# post-copy digest overwrote the pre-copy one and was never compared, so a mixed capture was published
# STAMPED WITH THE FINAL DIGEST — and every later poll matched that digest and reused it.
if [ "$HAVE_REAL_CP" -eq 1 ]; then
  reset_stub
  mut_dir="$snap_repo/.roborev/roborev-snapshot-909090"
  mut_log="$tmp/cx31w-transcript.log"
  CP_SHIM_MODE=mutate-source
  CP_SHIM_WITNESS="$tmp/cx31w-cp-witness"
  : >"$CP_SHIM_WITNESS"
  STUB_SNAPSHOT_LIFECYCLE_DIR="$mut_dir"
  STUB_SNAPSHOT_PATHS='alpha.rs beta.rs'
  STUB_SNAPSHOT_EXPECT_NO_CAPTURE=1
  STUB_ANNOUNCE_SHA=$(git -C "$snap_work" rev-parse HEAD)
  STUB_PROMPT=$(snap_prompt "$mut_dir/roborev-snapshot-content.diff")
  WRAPPER_PATH_PREFIX="$cpshim"
  run_wrapper "$snap_work" --log "$mut_log"
  WRAPPER_PATH_PREFIX=""
  # THE WATCHER DEMONSTRABLY TRIED — without this the absence below would be a timeout, not a finding.
  if [ -s "$CP_SHIM_WITNESS" ]; then
    ok "case (cx31w): the interposed copy ran ($(wc -l <"$CP_SHIM_WITNESS" | tr -d '[:space:]') attempt(s)), so the discard is a measured outcome"
  else
    bad 'case (cx31w): the interposed copy never ran, so nothing below can be concluded'
  fi
  if [ -s "$INVOKED.capture-seen" ]; then
    bad 'case (cx31w): a capture was PUBLISHED from a source that changed during the copy — the staged bytes may be a mix of two versions and every later poll would reuse them (#3312 job 8)'
  else
    ok 'case (cx31w): the mid-copy mutation was discarded, never published'
  fi
  assert_verdict 'case (cx31w)' FAIL 1
  assert_says 'case (cx31w) with nothing publishable the cause names the ORACLE, not the branch' \
    '^prompt-content: FAIL \(snapshot diff unusable: cleaned-up\)$'
  assert_lacks 'case (cx31w) a mixed capture never certifies the census' '^prompt-content: PASS'
  reset_stub
else
  printf 'SKIP - case (cx31w): no /bin/cp to delegate to\n'
fi

printf '== (cx31x) a copy that does not reproduce the source is DISCARDED (the STAGED digest leg) ==\n'
# The leg the pre/post comparison cannot see: the source is untouched (pre == post) while the staged
# copy holds fewer bytes. Only digesting the STAGED FILE catches it. Without that leg the truncated
# capture is published and `prompt-content:` reports the missing path as though the BRANCH had not
# delivered it — a FAIL that blames the diff for the oracle's defect.
if [ "$HAVE_REAL_CP" -eq 1 ]; then
  reset_stub
  trunc_dir="$snap_repo/.roborev/roborev-snapshot-919191"
  trunc_log="$tmp/cx31x-transcript.log"
  CP_SHIM_MODE=truncate-copy
  CP_SHIM_WITNESS="$tmp/cx31x-cp-witness"
  : >"$CP_SHIM_WITNESS"
  STUB_SNAPSHOT_LIFECYCLE_DIR="$trunc_dir"
  STUB_SNAPSHOT_PATHS='alpha.rs beta.rs'
  STUB_SNAPSHOT_EXPECT_NO_CAPTURE=1
  STUB_ANNOUNCE_SHA=$(git -C "$snap_work" rev-parse HEAD)
  STUB_PROMPT=$(snap_prompt "$trunc_dir/roborev-snapshot-content.diff")
  WRAPPER_PATH_PREFIX="$cpshim"
  run_wrapper "$snap_work" --log "$trunc_log"
  WRAPPER_PATH_PREFIX=""
  if [ -s "$CP_SHIM_WITNESS" ]; then
    ok 'case (cx31x): the interposed copy ran, so the discard is a measured outcome'
  else
    bad 'case (cx31x): the interposed copy never ran, so nothing below can be concluded'
  fi
  if [ -s "$INVOKED.capture-seen" ]; then
    bad 'case (cx31x): a capture whose staged bytes differ from the source was PUBLISHED (#3312 job 8, staged-digest leg)'
  else
    ok 'case (cx31x): the short copy was discarded, never published'
  fi
  assert_verdict 'case (cx31x)' FAIL 1
  assert_says 'case (cx31x) the cause names the oracle rather than blaming the branch for a missing path' \
    '^prompt-content: FAIL \(snapshot diff unusable: cleaned-up\)$'
  assert_lacks 'case (cx31x) the truncated capture never becomes an absent-path verdict against the diff' \
    'code census paths absent from'
  reset_stub
else
  printf 'SKIP - case (cx31x): no /bin/cp to delegate to\n'
fi

printf '== (cx31y) an UNMEASURABLE digest never satisfies the three-way agreement (job 8) ==\n'
# THE EMPTY-EQUALS-EMPTY TRAP, checked rather than asserted in prose: three digests that must AGREE are
# trivially satisfiable if a failed measurement yields an empty string on every leg. This runs a PATCHED
# COPY of the flow scripts whose digest helper SUCCEEDS while returning nothing — the shape a renamed or
# missing `sha256sum`/`shasum`/`cksum` would produce if the helper's status were ever loosened — and
# requires that no capture is published. cx31r is the control: the unpatched copy publishes and PASSes on
# this same fixture.
_dg_dir="$tmp/digest-mutant"
mkdir -p "$_dg_dir"
cp "$SCRIPT_DIR/../flow/roborev-review.sh" "$SCRIPT_DIR/../flow/roborev-review-oracles.sh" \
  "$SCRIPT_DIR/../flow/roborev-review-checks.sh" "$_dg_dir/"
[ -f "$SCRIPT_DIR/../flow/roborev-job-facts.py" ] && cp "$SCRIPT_DIR/../flow/roborev-job-facts.py" "$_dg_dir/"
reset_stub
_dg_real_wrapper="$WRAPPER"
_dg_dir_snap="$snap_repo/.roborev/roborev-snapshot-939393"
_dg_log="$tmp/cx31y-transcript.log"
# THE CONTROL FIRST: the UNPATCHED copy must publish a capture and PASS, or a later absence proves nothing.
WRAPPER="$_dg_dir/roborev-review.sh"
STUB_SNAPSHOT_LIFECYCLE_DIR="$_dg_dir_snap"
STUB_SNAPSHOT_PATHS='alpha.rs beta.rs'
STUB_ANNOUNCE_SHA=$(git -C "$snap_work" rev-parse HEAD)
STUB_PROMPT=$(snap_prompt "$_dg_dir_snap/roborev-snapshot-content.diff")
run_wrapper "$snap_work" --log "$_dg_log"
assert_verdict 'case (cx31y control) the UNPATCHED copy captures and PASSes' PASS 0
# Now make every digest read SUCCEED WITH AN EMPTY VALUE.
if sed_inplace_verified "$_dg_dir/roborev-review-oracles.sh" \
  's/^  \[ -n "\$out" \] || return 1$/  out=""/' \
  '  out=""' '  [ -n "$out" ] || return 1'; then
  ok 'case (cx31y): the empty-digest patch was really applied to the copy'
  reset_stub
  _dg_dir_snap2="$snap_repo/.roborev/roborev-snapshot-949494"
  _dg_log2="$tmp/cx31y2-transcript.log"
  STUB_SNAPSHOT_LIFECYCLE_DIR="$_dg_dir_snap2"
  STUB_SNAPSHOT_PATHS='alpha.rs beta.rs'
  STUB_SNAPSHOT_EXPECT_NO_CAPTURE=1
  CP_SHIM_WITNESS="$tmp/cx31y-nocp-witness"
  printf 'no interposed copy in this case; the wait falls back to one poll\n' >"$CP_SHIM_WITNESS"
  STUB_ANNOUNCE_SHA=$(git -C "$snap_work" rev-parse HEAD)
  STUB_PROMPT=$(snap_prompt "$_dg_dir_snap2/roborev-snapshot-content.diff")
  run_wrapper "$snap_work" --log "$_dg_log2"
  if [ -s "$INVOKED.capture-seen" ]; then
    bad 'case (cx31y): a capture was published while EVERY digest was unmeasurable — three empty strings compared equal and satisfied the agreement (#3312 job 8)'
  else
    ok 'case (cx31y): an unmeasurable digest is refused, never treated as agreement'
  fi
  assert_verdict 'case (cx31y)' FAIL 1
  assert_says 'case (cx31y) the fail-closed cause is the absent capture, not a silent pass' \
    '^prompt-content: FAIL \(snapshot diff unusable: cleaned-up\)$'
else
  bad 'case (cx31y): could not patch the copied oracles, so the empty-digest path was never exercised'
fi
WRAPPER="$_dg_real_wrapper"
reset_stub

printf '== (cx31z) a TMPDIR that resolves INSIDE the repo is refused, and named (job 9, F2) ==\n'
# A LEXICAL containment test passes a `TMPDIR` whose path TRAVERSES A SYMLINK into the checkout: the
# string does not start with $REPO, but the directory is inside it. Our own copies would then live in the
# tree the census and the watcher are both reading. The fixture is exactly that shape — a symlink outside
# the repo pointing at a directory inside it — and the refusal must be NAMED, because a silently absent
# capture would leave an operator staring at `cleaned-up` with no idea why.
reset_stub
tmp_inside="$snap_repo/inside-the-checkout-tmp"
mkdir -p "$tmp_inside"
tmp_symlink="$tmp/tmpdir-that-resolves-into-the-repo"
rm -rf "$tmp_symlink"
ln -s "$tmp_inside" "$tmp_symlink"
hostile_dir="$snap_repo/.roborev/roborev-snapshot-969696"
hostile_log="$tmp/cx31z-transcript.log"
STUB_SNAPSHOT_LIFECYCLE_DIR="$hostile_dir"
STUB_SNAPSHOT_PATHS='alpha.rs beta.rs'
STUB_SNAPSHOT_EXPECT_NO_CAPTURE=1
CP_SHIM_WITNESS="$tmp/cx31z-witness"
printf 'no interposed copy here; the wait falls back to one poll\n' >"$CP_SHIM_WITNESS"
STUB_ANNOUNCE_SHA=$(git -C "$snap_work" rev-parse HEAD)
STUB_PROMPT=$(snap_prompt "$hostile_dir/roborev-snapshot-content.diff")
WRAPPER_TMPDIR="$tmp_symlink"
run_wrapper "$snap_work" --log "$hostile_log"
WRAPPER_TMPDIR=""
assert_verdict 'case (cx31z)' FAIL 1
assert_says 'case (cx31z) the refusal is NAMED, with the resolved path and the repo' \
  "^NOTICE: snapshot-capture: the private capture directory resolves INSIDE the reviewed repository \('$snap_repo/inside-the-checkout-tmp/roborev-snapshot-capture\.[A-Za-z0-9]+' is under '$snap_repo'"
assert_says 'case (cx31z) the notice says what to do about it' 'Point TMPDIR outside the checkout and re-run'
assert_says 'case (cx31z) and that the consequence is fail-closed, not a silent pass' \
  'fail-closed, never a silent pass'
assert_says 'case (cx31z) the run itself fails closed on the absent capture' \
  '^prompt-content: FAIL \(snapshot diff unusable: cleaned-up\)$'
# AND NOTHING OF OURS WAS LEFT INSIDE THE REVIEWED TREE — the point of the refusal.
if ls -d "$tmp_inside"/roborev-snapshot-capture.* >/dev/null 2>&1; then
  bad "case (cx31z): a capture directory was left INSIDE the reviewed repository ($(ls -d "$tmp_inside"/roborev-snapshot-capture.* 2>/dev/null | head -1))"
else
  ok 'case (cx31z): no capture directory survives inside the reviewed repository'
fi
rm -rf "$tmp_inside" "$tmp_symlink"
reset_stub

printf '== (cx32) a NON-REGULAR live path is reported unreadable, never answered from a capture (job 10) ==\n'
# THE AFFIRMATIVE-MEASUREMENT DEFECT IN ITS PUREST FORM: the fallback was keyed on `! -f` — "anything
# other than a regular file" — so a DIRECTORY or FIFO at the named path (not "the snapshot is gone", but
# "the named path is something we cannot read") silently took the capture branch and could PASS. The
# fixture makes the capture VALID and COMPLETE, so only the predicate stands between it and a green.
reset_stub
nonreg_dir="$snap_repo/.roborev/roborev-snapshot-323232"
nonreg_log="$tmp/cx32-transcript.log"
STUB_SNAPSHOT_LIFECYCLE_DIR="$nonreg_dir"
STUB_SNAPSHOT_PATHS='alpha.rs beta.rs'
STUB_SNAPSHOT_REPLACE_WITH=fifo
STUB_ANNOUNCE_SHA=$(git -C "$snap_work" rev-parse HEAD)
STUB_PROMPT=$(snap_prompt "$nonreg_dir/roborev-snapshot-content.diff")
run_wrapper "$snap_work" --log "$nonreg_log"
# The capture really was published — otherwise the refusal below would be vacuous.
if [ -s "$INVOKED.capture-seen" ]; then
  ok 'case (cx32): a valid capture was published and was still not substituted for the unreadable path'
else
  bad 'case (cx32): no capture was published, so the refusal below is reached with nothing to substitute'
fi
assert_verdict 'case (cx32)' FAIL 1
assert_says 'case (cx32) an existing-but-unreadable live path is reported as such' \
  '^prompt-content: FAIL \(snapshot diff unusable: unreadable\)$'
assert_says 'case (cx32) the cause distinguishes unreadable from deleted' \
  'a path that exists as something unreadable is a different fact from a snapshot that was deleted'
assert_lacks 'case (cx32) the capture never answers for an unreadable live path' '^prompt-content: PASS'
rm -rf "$nonreg_dir"
reset_stub

printf "== (cx32b) the resolver takes the NEWEST publication for an id (direct unit probe) ==\n"
# Publications are append-only per run (`pub.<id>.<seq>`), so "which one" is decided by the sequence.
# Probed directly: two publications for one id, the older one carrying a path the prompt does NOT name.
# Picking the older would certify a diff the reviewer was not pointed at — the same class as the sibling
# substitution, one level along.
cx32b_probe="$tmp/cx32b-probe.sh"
cx32b_out="$tmp/cx32b-out.txt"
cat >"$cx32b_probe" <<'CX32B'
set -uo pipefail
REPO="$3"
. "$1"
. "$2"
LOG="$4/cx32b.log"
PROMPT_FILE="$LOG.prompt"
ROBOREV_SNAPSHOT_CAPTURE_DIR="$4/cx32b-capture"
rel=".roborev/roborev-snapshot-565656/roborev-snapshot-content.diff"
# OLDER publication: identifies a DIFFERENT file, and would be rejected as a path mismatch if chosen.
mkdir -p "$ROBOREV_SNAPSHOT_CAPTURE_DIR/pub.roborev-snapshot-565656.000001"
printf 'diff --git a/stale.rs b/stale.rs\n' >"$ROBOREV_SNAPSHOT_CAPTURE_DIR/pub.roborev-snapshot-565656.000001/content.diff"
printf 'rel=%s\ndigest=old\n' ".roborev/roborev-snapshot-565656/some-other.diff" >"$ROBOREV_SNAPSHOT_CAPTURE_DIR/pub.roborev-snapshot-565656.000001/meta"
# NEWER publication: the real one.
mkdir -p "$ROBOREV_SNAPSHOT_CAPTURE_DIR/pub.roborev-snapshot-565656.000002"
printf 'diff --git a/alpha.rs b/alpha.rs\n' >"$ROBOREV_SNAPSHOT_CAPTURE_DIR/pub.roborev-snapshot-565656.000002/content.diff"
printf 'rel=%s\ndigest=new\n' "$rel" >"$ROBOREV_SNAPSHOT_CAPTURE_DIR/pub.roborev-snapshot-565656.000002/meta"
printf 'Read the diff from: `%s`\n' "$REPO/$rel" >"$PROMPT_FILE"
CENSUS='1 file, +1/-0'
BASE='origin/main'
JOB=11
census_code_paths=(alpha.rs)
DETAILS=()
PROMPT_CONTENT=""
roborev_check_prompt_content
printf 'prompt-content: %s\n' "$PROMPT_CONTENT"
CX32B
mkdir -p "$tmp/cx32b-repo/.roborev"
if bash "$cx32b_probe" "$ORACLES_SRC" "$CHECKS_SRC" "$tmp/cx32b-repo" "$tmp" >"$cx32b_out" 2>&1; then
  if grep -qE '^prompt-content: PASS \(1/1 code census paths present in the captured snapshot diff\)$' "$cx32b_out"; then
    ok 'case (cx32b): the newest publication is the one read'
  else
    bad "case (cx32b): expected the newest publication to be read, got '$(head -2 "$cx32b_out")'"
  fi
else
  bad "case (cx32b): the unit probe did not run: $(cat "$cx32b_out")"
fi

printf "== (cx32c) the watcher's KILL escalation cannot abort the wrapper (job 11, direct probe) ==\n"
# THE LOUD DIRECTION OF THIS ISSUE'S OWN DEFECT: `kill -0` can succeed and the watcher can exit before
# `kill -KILL` runs. `kill -KILL` was the LAST command of that AND-OR list, so its failure tripped
# `set -e` and ABORTED the wrapper — a spurious non-PASS on a genuine review. Probed directly, with
# `kill` shimmed to reproduce exactly that interleaving (alive for `-0`, gone for `-KILL`): constructing
# a real process that dies inside the window would be a race, and a racy test for a race is no evidence.
cx32c_probe="$tmp/cx32c-probe.sh"
cx32c_out="$tmp/cx32c-out.txt"
cat >"$cx32c_probe" <<'CX32C'
set -euo pipefail
REPO="$2"
. "$1"
# The interleaving, deterministically: TERM and -0 succeed (the watcher looks alive for the whole grace
# window), then the process exits, so -KILL fails.
kill() {
  case "${1:-}" in
    -TERM) return 0 ;;
    -0) return 0 ;;
    -KILL) return 1 ;;
    *) return 0 ;;
  esac
}
sleep() { return 0; }
wait() { return 1; }
ROBOREV_SNAPSHOT_CAPTURE_PID=999999
ROBOREV_SNAPSHOT_CAPTURE_DIR=""
roborev_snapshot_capture_stop
printf 'stop-returned-%s\n' "$?"
printf 'wrapper-survived\n'
CX32C
if bash "$cx32c_probe" "$ORACLES_SRC" "$tmp" >"$cx32c_out" 2>&1; then
  if grep -qF 'wrapper-survived' "$cx32c_out"; then
    ok 'case (cx32c): a watcher that exits inside the escalation window does not abort the wrapper'
  else
    bad "case (cx32c): stop() did not return control — the escalation aborted under set -e: $(cat "$cx32c_out")"
  fi
  if grep -qF 'stop-returned-0' "$cx32c_out"; then
    ok 'case (cx32c): stop() reports success, so no caller sees a spurious failure either'
  else
    bad "case (cx32c): stop() returned non-zero, which under set -e aborts its caller: $(cat "$cx32c_out")"
  fi
else
  bad "case (cx32c): the probe exited non-zero — the KILL escalation aborted the wrapper (#3312 job 11): $(cat "$cx32c_out")"
fi

printf '== (cx32d) a `..` traversal between snapshot directories is refused (job 12) ==\n'
# VALIDATE BEFORE YOU NORMALISE. The path nominally names snapshot A and traverses into snapshot B, and
# BOTH directories exist — so physical resolution erases the `..`, every component check then inspects
# B's components, and the binding reports B while the prompt named A. The fixture makes B's capture the
# complete one, so an accepted traversal would certify a diff from a snapshot the prompt did not name.
reset_stub
trav_a="$snap_repo/.roborev/roborev-snapshot-A11111"
trav_b="$snap_repo/.roborev/roborev-snapshot-B22222"
mkdir -p "$trav_a" "$trav_b"
write_snap_diff "$trav_b/roborev-snapshot-content.diff" alpha.rs beta.rs
STUB_ANNOUNCE_SHA=$(git -C "$snap_work" rev-parse HEAD)
STUB_PROMPT=$(snap_prompt "$trav_a/../roborev-snapshot-B22222/roborev-snapshot-content.diff")
run_wrapper "$snap_work"
assert_verdict 'case (cx32d)' FAIL 1
assert_says 'case (cx32d) the traversal is refused as unbound' \
  '^prompt-content: FAIL \(snapshot diff unusable: unbound-job\)$'
assert_says 'case (cx32d) the cause names the dot segment and what resolution would have done' \
  "the path contains a '\.\.' segment, which resolution would erase"
assert_lacks 'case (cx32d) a traversal never certifies from the directory it lands in' '^prompt-content: PASS'
rm -rf "$trav_a" "$trav_b"
reset_stub

printf '== (cx32e) a snapshot DIRECTORY symlink is refused, not resolved away (job 12) ==\n'
# The other half of the same defect: `.roborev/roborev-snapshot-C -> roborev-snapshot-D`. Resolution
# rewrites the path into D, so the id, the relative path and the capture lookup would all silently
# become D's while the prompt named C. Checked on the path's OWN components instead.
reset_stub
sym_c="$snap_repo/.roborev/roborev-snapshot-C33333"
sym_d="$snap_repo/.roborev/roborev-snapshot-D44444"
mkdir -p "$sym_d"
rm -rf "$sym_c"
write_snap_diff "$sym_d/roborev-snapshot-content.diff" alpha.rs beta.rs
ln -s "$sym_d" "$sym_c"
STUB_ANNOUNCE_SHA=$(git -C "$snap_work" rev-parse HEAD)
STUB_PROMPT=$(snap_prompt "$sym_c/roborev-snapshot-content.diff")
run_wrapper "$snap_work"
assert_verdict 'case (cx32e)' FAIL 1
assert_says 'case (cx32e) the directory symlink is refused' \
  '^prompt-content: FAIL \(snapshot diff unusable: symlinked\)$'
assert_says 'case (cx32e) the cause names which component is the symlink, or that the tail was rewritten' \
  "(a traversed DIRECTORY component is a SYMLINK|are not the ones it actually resolves to)"
assert_lacks 'case (cx32e) a symlinked snapshot dir never certifies from its target' '^prompt-content: PASS'
rm -rf "$sym_c" "$sym_d"
reset_stub

printf '== (cx31q) the COMPACT instruction spelling is read too ==\n'
reset_stub
write_snap_diff "$snap_file" alpha.rs beta.rs
STUB_ANNOUNCE_SHA=$(git -C "$snap_work" rev-parse HEAD)
# MEASURED IN THE INSTALLED BINARY, not inferred from one transcript: roborev v0.61.2 carries TWO
# instruction spellings — the full 'Read the diff from: `%s`' and a compact
# '(Diff too large; read `%s`.)' emitted under a tight prompt budget. Reading only the first
# would leave the same false FAIL in the tier that uses the second.
STUB_PROMPT="Review the change.\\n\\n(Diff too large; read \`$snap_file\`.)"
run_wrapper "$snap_work"
assert_verdict 'case (cx31q)' PASS 0
assert_says 'case (cx31q) the compact instruction resolves to the same snapshot source' \
  '^prompt-content: PASS \(2/2 code census paths present in the snapshot diff\)$'

printf '== (cx31q2) the DELEGATED-INSPECTION oversize tier is named, not mistaken for an empty prompt ==\n'
reset_stub
STUB_ANNOUNCE_SHA=$(git -C "$snap_work" rev-parse HEAD)
# roborev's OTHER oversize tiers (its codex_*/generic_* fallback templates, measured in the same
# binary) ship neither the diff nor a snapshot: they tell the reviewer to run git itself. Nothing
# obtainable locally can establish what such a review received, so the verdict stays FAIL — but
# the cause must say WHICH mode this is, or an operator hunts the wrong defect.
STUB_PROMPT='Review the change.\n\n### Combined Diff\n\n(Diff too large to include inline)\nFor Codex in read-only review mode, inspect the commit range locally before writing findings.\n- `git diff --stat`\n- `git diff`'
run_wrapper "$snap_work"
assert_verdict 'case (cx31q2)' FAIL 1
assert_says 'case (cx31q2) the no-source verdict is unchanged' \
  '^prompt-content: FAIL \(2/2 code census paths absent from the prompt\)$'
assert_says 'case (cx31q2) the delegated-inspection tier is named as the cause' \
  '^ERROR: prompt-content: the prompt carries a .\(Diff too large. notice but NO snapshot path'
assert_says 'case (cx31q2) and it says why nothing local can verify it' \
  'NOTHING obtainable locally can establish which files the reviewer actually looked at'

printf '== (cx31p) a READABLE instruction line does not excuse an UNREADABLE one ==\n'
reset_stub
write_snap_diff "$snap_file" alpha.rs beta.rs
STUB_ANNOUNCE_SHA=$(git -C "$snap_work" rev-parse HEAD)
# One instruction line resolves to a perfectly good snapshot; a second carries no readable path.
# Taking the good one would be the "only the bad states are tested" shape with the roles swapped:
# an unread instruction line leaves an UNKNOWN source, and a readable sibling cannot know it.
STUB_PROMPT="$(snap_prompt "$snap_file")\\nRead the diff from: somewhere else entirely"
run_wrapper "$snap_work"
assert_verdict 'case (cx31p)' FAIL 1
assert_says 'case (cx31p) the unreadable instruction line still FAILs the run' \
  '^prompt-content: FAIL \(snapshot diff unusable: unparseable-path\)$'
assert_lacks 'case (cx31p) the readable sibling never excuses it' '^prompt-content: PASS'

printf '== (cx31o) a snapshot instruction inside a DIFF BODY is not a snapshot path ==\n'
reset_stub
# The anchor is COLUMN ZERO. Every diff body line carries a leading `+`/`-`/space, so prose in
# the reviewed change that quotes roborev's own instruction (this repo's docs and tests do)
# can never be mistaken for the instruction itself — otherwise a branch could name any file it
# liked as the oracle for its own review.
rm -rf "$snap_dir"
STUB_ANNOUNCE_SHA=$(git -C "$snap_work" rev-parse HEAD)
STUB_PROMPT='Review the change.\ndiff --git a/alpha.rs b/alpha.rs\n+Read the diff from: `/etc/passwd`\ndiff --git a/beta.rs b/beta.rs\n @@ x @@'
run_wrapper "$snap_work"
assert_verdict 'case (cx31o)' PASS 0
assert_says 'case (cx31o) the inline diff is what is read, with no snapshot involved' \
  '^prompt-content: PASS \(2/2 code census paths present\)$'
assert_lacks 'case (cx31o) the quoted instruction is not resolved as a snapshot' 'snapshot diff unusable'

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
# #3312: the second diff-delivery mode is part of the documented contract — an operator reading
# a `snapshot diff unusable:` FAIL must be able to learn from --help what the check now reads and
# that the snapshot directory is transient.
assert_says '--help documents BOTH diff-delivery modes' 'TWO DIFF-DELIVERY MODES'
assert_says '--help names the transient snapshot directory' 'roborev-snapshot-<id>'
assert_says '--help says a large review must no longer false-FAIL' 'no longer FALSE-FAILs'
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
FLOW_FILES=("$ORACLES" "$CHECKS_FILE" "$WRAPPER")
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
_unq_callers=$(grep -nE '(^|[^#])roborev_unquote_path ' "$ORACLES" "$CHECKS_FILE" "$WRAPPER" \
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
if [ "${_coll_defs:-0}" -eq 1 ] && grep -qE '^ *roborev_collect_review_diff_headers "\$PROMPT_FILE"' "$CHECKS_FILE"; then
  ok 'structural: the prompt headers (and their rename from/to lines) are collected by the oracles file'
else
  bad "structural: roborev_collect_prompt_headers is not the single collector (defs in oracles: ${_coll_defs:-0}) or prompt-content does not go through the oracles' diff-source resolver"
fi
# (6) THE SNAPSHOT DELIVERY MODE LIVES WITH THE HEADER COLLECTOR TOO (#3312). roborev delivers a
#     large diff by writing it to a transient snapshot file and naming that path in the prompt,
#     so "where the diff actually is" is prompt-shape knowledge of exactly the kind (5) keeps out
#     of the checks file. The resolver is therefore defined ONCE in the oracles file, the checks
#     file calls it and does no prompt scanning of its own, and the repo/job binding is asserted
#     to exist in the resolver — a resolver that stopped comparing against $REPO would let a
#     stale snapshot from another checkout certify this review.
_src_defs=$(grep -cE '^roborev_collect_review_diff_headers\(\) \{' "$ORACLES" || true)
if [ "${_src_defs:-0}" -eq 1 ]; then
  ok 'structural: roborev_collect_review_diff_headers is defined exactly once, in the oracles file'
else
  bad "structural: expected exactly 1 definition of roborev_collect_review_diff_headers in the oracles file, found ${_src_defs:-0}"
fi
_snap_defs=$(grep -cE '^roborev_prompt_snapshot_paths\(\) \{' "$ORACLES" || true)
if [ "${_snap_defs:-0}" -eq 1 ]; then
  ok 'structural: the snapshot-path extraction is defined exactly once, in the oracles file'
else
  bad "structural: expected exactly 1 definition of roborev_prompt_snapshot_paths in the oracles file, found ${_snap_defs:-0}"
fi
if grep -nE '(grep|awk|sed)[^#]*Read the diff from' "$CHECKS_FILE" 2>/dev/null | grep -qv '^[0-9]*: *#'; then
  bad 'structural: roborev-review-checks.sh scans the prompt for the snapshot instruction itself — prompt-shape knowledge must stay with the collector in the oracles file (#3312)'
else
  ok 'structural: roborev-review-checks.sh does no snapshot-instruction scanning of its own'
fi
# The extraction must be anchored at COLUMN ZERO: a diff body line always carries a leading
# `+`/`-`/space, so an unanchored match would let the reviewed change name its own oracle.
if grep -qF 'index($0, "Read the diff from:") == 1' "$ORACLES"; then
  ok 'structural: the snapshot instruction is matched at column zero (a quoted line in a diff body cannot pose as it)'
else
  bad 'structural: the snapshot-instruction match is not anchored at column zero — prose inside the reviewed diff could name the file its own review is judged against (#3312)'
fi
# VALIDATION PRECEDES NORMALISATION IN THE BINDING (job 12), asserted by LINE ORDER: physical
# resolution erases `..` segments and directory symlinks, which is exactly what the component checks
# exist to find, so a resolution that runs first makes them inspect a path the prompt never named.
_vb_s=$(grep -nE '^roborev_snapshot_path_binding\(\) \{' "$ORACLES" | head -1 | cut -d: -f1)
_vb_e=""
[ -z "$_vb_s" ] || _vb_e=$(awk -v s="$_vb_s" 'NR>s && /^}/ {print NR; exit}' "$ORACLES")
if [ -z "$_vb_s" ] || [ -z "$_vb_e" ]; then
  bad 'structural: could not locate roborev_snapshot_path_binding to inspect its ordering'
else
  _vb_body=$(sed -n "${_vb_s},${_vb_e}p" "$ORACLES")
  _vb_dot=$(printf '%s\n' "$_vb_body" | grep -nF "'.'|'..')" | head -1 | cut -d: -f1)
  _vb_res=$(printf '%s\n' "$_vb_body" | grep -nF '_roborev_resolve_existing_ancestor "$p"' | head -1 | cut -d: -f1)
  _vb_tail=$(printf '%s\n' "$_vb_body" | grep -nF 'if [ "$orig_tail" != "$rel" ]' | head -1 | cut -d: -f1)
  if [ -n "$_vb_dot" ] && [ -n "$_vb_res" ] && [ "$_vb_dot" -lt "$_vb_res" ]; then
    ok 'structural: dot-segment validation precedes physical resolution in the snapshot binding'
  else
    bad "structural: the binding resolves before it validates dot segments (dot at ${_vb_dot:-<absent>}, resolve at ${_vb_res:-<absent>}) — a '..' traversal between snapshot directories would be normalised away (#3312 job 12)"
  fi
  if [ -n "$_vb_tail" ]; then
    ok "structural: the binding requires the path's OWN tail components to equal the resolved ones (a directory symlink cannot rewrite them)"
  else
    bad 'structural: the binding no longer compares the original tail with the resolved one — a directory symlink would silently rebind the snapshot id (#3312 job 12)'
  fi
  if printf '%s\n' "$_vb_body" | grep -qF 'if [ -L "$walk" ]; then'; then
    ok 'structural: the binding rejects a symlink at EVERY traversed component, on the original spelling'
  else
    bad 'structural: the binding checks only the final component for a symlink — a symlinked .roborev or snapshot directory would be traversed (#3312 job 12)'
  fi
fi
_bind_start=$(grep -nE '^roborev_snapshot_path_binding\(\) \{' "$ORACLES" | head -1 | cut -d: -f1)
if [ -z "$_bind_start" ]; then
  bad 'structural: roborev_snapshot_path_binding is not defined — the snapshot path would be read without being bound to the reviewed repo (#3312)'
else
  _bind_end=$(awk -v s="$_bind_start" 'NR>s && /^}/ {print NR; exit}' "$ORACLES")
  if [ -z "$_bind_end" ] || [ "$_bind_end" -le "$_bind_start" ]; then
    bad "structural: the snapshot-binding body bounds could not be resolved (start $_bind_start, end '${_bind_end:-<none>}') — the asserts below would scan nothing"
  else
    _bind_body=$(sed -n "${_bind_start},${_bind_end}p" "$ORACLES")
    if printf '%s\n' "$_bind_body" | grep -qE '\$\{?REPO' \
      && printf '%s\n' "$_bind_body" | grep -qF 'foreign-repo'; then
      ok "structural: the snapshot path is bound to the reviewed repo \$REPO, with a foreign-repo verdict (lines $_bind_start-$_bind_end)"
    else
      bad 'structural: the snapshot binding no longer derives its containment test from $REPO (or has no foreign-repo verdict) — a snapshot from another checkout could satisfy prompt-content: (#3312)'
    fi
    if printf '%s\n' "$_bind_body" | grep -qF 'roborev-snapshot-'; then
      ok 'structural: the snapshot path must sit under a roborev-snapshot-<id> directory'
    else
      bad 'structural: the snapshot binding no longer requires the roborev-snapshot-<id> directory shape — any in-repo file could pose as the review oracle (#3312)'
    fi
  fi
fi
# (7) CAPTURE PROVENANCE IS ESTABLISHED AT CAPTURE TIME AND KEYED BY THE WHOLE PATH (roborev job 6).
#     Behavioural cases cx31s/cx31t/cx31u prove the three refusals fire; only a structural assert can
#     pin the ORDERING and the AFFIRMATIVE requirement, which is where both blockers lived: `-f` and
#     `cp` follow symlinks, so a validation that runs after the copy — or after roborev has deleted the
#     original — cannot see what was followed.
# THE CAPTURE DIRECTORY IS PRIVATE AND PER-RUN (job 7, finding 1): created by `mktemp -d`, refused if it
# resolves inside the reviewed repo, and removed at exit. A shared `mkdir -p` path is reusable across
# runs, which is the staleness/planting class this removes BY CONSTRUCTION rather than by checking.
if grep -qE 'mktemp -d "\$\{TMPDIR:-/tmp\}/roborev-snapshot-capture' "$ORACLES"; then
  ok 'structural: the capture directory is created by mktemp -d (private, per-run, unpredictable)'
else
  bad 'structural: the capture directory is not a per-run mktemp -d — a shared, guessable path can be reused across runs and predate one (#3312 job 7, finding 1)'
fi
# THE CONTAINMENT TEST IS DECIDED ON A PHYSICALLY RESOLVED PATH (job 9, F2): a lexical test admits a
# relative TMPDIR and one whose path traverses a symlink into the checkout.
_start_s=$(grep -nE '^roborev_snapshot_capture_start\(\) \{' "$ORACLES" | head -1 | cut -d: -f1)
_start_e=""
[ -z "$_start_s" ] || _start_e=$(awk -v s="$_start_s" 'NR>s && /^}/ {print NR; exit}' "$ORACLES")
if [ -z "$_start_s" ] || [ -z "$_start_e" ]; then
  bad 'structural: could not locate roborev_snapshot_capture_start to inspect its containment test'
else
  _start_body=$(sed -n "${_start_s},${_start_e}p" "$ORACLES")
  _sb_missing=""
  printf '%s\n' "$_start_body" | grep -qF 'pwd -P' || _sb_missing="$_sb_missing physical-resolution"
  printf '%s\n' "$_start_body" | grep -qF '"$phys/" in' || _sb_missing="$_sb_missing containment-on-resolved-path"
  printf '%s\n' "$_start_body" | grep -qF 'ROBOREV_SNAPSHOT_CAPTURE_DIR="$phys"' || _sb_missing="$_sb_missing stores-resolved-path"
  printf '%s\n' "$_start_body" | grep -qE 'case "\$phys" in' || _sb_missing="$_sb_missing absolute-check"
  if [ -z "$_sb_missing" ]; then
    ok 'structural: containment is decided on the pwd -P-resolved path, which is also what gets stored'
  else
    bad "structural: the capture-directory containment test is incomplete —$_sb_missing. A lexical test admits a relative TMPDIR or one symlinked into the checkout (#3312 job 9, F2)"
  fi
fi
# THE TEST HARNESS'S OWN DISCOVERY MUST BE PER-PROCESS (job 9, F3): a glob over the shared temp dir can
# be satisfied by a concurrent run's capture, which is this change's own false-PASS class one layer down.
# THE PROPERTY IS "no discovery searches a location SHARED with other runs", not "every glob names one
# variable": a glob rooted at a path inside this process's own `$tmp` (the fixture repo, for instance) is
# already unique. What must never appear is a glob rooted at the SHARED temp dir — `${TMPDIR...}` or a
# literal `/tmp/` — since that is what another run's capture can satisfy.
_selfglobs=$(grep -n 'roborev-snapshot-capture\.' "$0" \
  | grep -E '(\$\{TMPDIR|"/tmp/|[^a-zA-Z_]/tmp/)' | grep -v '^[0-9]*: *#' || true)
if [ -z "$_selfglobs" ]; then
  ok 'structural: no capture discovery in this suite searches the SHARED temp dir (per-process scoping only)'
else
  bad "structural: a capture glob in this suite searches the shared temp dir: ${_selfglobs%%$'\n'*} — a concurrent run's artifact could satisfy a wait (#3312 job 9, F3)"
fi
if grep -qE '^CAPTURES_PARENT="\$tmp/captures"$' "$0" && grep -qF 'TMPDIR="${WRAPPER_TMPDIR:-$CAPTURES_PARENT}"' "$0"; then
  ok 'structural: wrapper runs are pinned to the per-process capture parent'
else
  bad 'structural: wrapper runs do not pin TMPDIR to a per-process capture parent (#3312 job 9, F3)'
fi
if grep -qE '^roborev_snapshot_capture_cleanup\(\) \{' "$ORACLES" \
  && grep -qF 'roborev_snapshot_capture_cleanup' "$WRAPPER"; then
  ok 'structural: the private capture directory is removed by a cleanup the wrapper calls'
else
  bad 'structural: nothing removes the private capture directory (#3312 job 7)'
fi
_cap_val_defs=$(grep -cE '^_roborev_capture_validate\(\) \{' "$ORACLES" || true)
if [ "${_cap_val_defs:-0}" -eq 1 ]; then
  ok 'structural: the capture-time validator is defined exactly once, in the oracles file'
else
  bad "structural: expected exactly 1 definition of _roborev_capture_validate, found ${_cap_val_defs:-0} — capture-time provenance has no single home (#3312)"
fi
_cap_loop_start=$(grep -nE '^_roborev_snapshot_capture_loop\(\) \{' "$ORACLES" | head -1 | cut -d: -f1)
if [ -z "$_cap_loop_start" ]; then
  bad 'structural: _roborev_snapshot_capture_loop is not defined — nothing captures the snapshot (#3312)'
else
  _cap_loop_end=$(awk -v s="$_cap_loop_start" 'NR>s && /^}/ {print NR; exit}' "$ORACLES")
  if [ -z "$_cap_loop_end" ] || [ "$_cap_loop_end" -le "$_cap_loop_start" ]; then
    bad "structural: the capture-loop bounds could not be resolved (start $_cap_loop_start, end '${_cap_loop_end:-<none>}') — the asserts below would scan nothing"
  else
    _cap_body=$(sed -n "${_cap_loop_start},${_cap_loop_end}p" "$ORACLES")
    # THE ORDERING, asserted by LINE NUMBER: every read of the source must come after the validation
    # call. A validator that exists but runs late is the defect, not the remedy.
    _val_line=$(printf '%s\n' "$_cap_body" | grep -nE '_roborev_capture_validate "\$repo"' | head -1 | cut -d: -f1)
    _cp_line=$(printf '%s\n' "$_cap_body" | grep -nE 'cp "\$f"' | head -1 | cut -d: -f1)
    if [ -n "$_val_line" ] && [ -n "$_cp_line" ] && [ "$_val_line" -lt "$_cp_line" ]; then
      ok 'structural: the capture loop validates BEFORE it reads the snapshot (cp follows symlinks, so order is the property)'
    else
      bad "structural: the capture loop does not validate before copying (validate at ${_val_line:-<absent>}, cp at ${_cp_line:-<absent>}) — a symlinked source would be followed and the evidence deleted before any later check (#3312 blocker 1)"
    fi
    # The published unit carries its own IDENTITY (`meta`: the captured relative path + digest) and is
    # published by ONE rename, so no observer can see content without identity — the SIGTERM race that
    # two separate writes had (job 7, finding 2) is unexpressible here.
    if printf '%s\n' "$_cap_body" | grep -qF 'stage/meta'; then
      ok 'structural: the capture loop stages an identity record beside the content'
    else
      bad 'structural: the capture loop writes no identity record, so a capture could not be matched to the path the prompt names (#3312)'
    fi
    if printf '%s\n' "$_cap_body" | grep -qE 'mv "\$stage" "\$out/\$dest"'; then
      ok 'structural: a capture is published by ONE rename of the staged directory (no half-published state)'
    else
      bad 'structural: the capture is not published by a single rename — an observer could see content without its identity, which is the SIGTERM false-FAIL race (#3312 job 7, finding 2)'
    fi
    # THE DESTINATION IS FRESH BY CONSTRUCTION (job 10, blocker 1). `mv <dir> <existing-dir>` does not
    # fail — it NESTS, leaving the previous content.diff/meta exactly where the resolver reads them. So
    # the property is not "check the rename": it is "there is no pre-existing destination", which also
    # means no rename-aside and no backup path exist to be checked.
    if printf '%s\n' "$_cap_body" | grep -qE "printf -v dest 'pub\.%s\.%06d'"; then
      ok 'structural: each capture publishes to a fresh pub.<id>.<seq> destination that cannot already exist'
    else
      bad 'structural: publications do not carry a per-run unique sequence — a fixed destination can be nested into rather than replaced (#3312 job 10, blocker 1)'
    fi
    if printf '%s\n' "$_cap_body" | grep -qE '\.old\.|mv "\$out/\$id"'; then
      bad 'structural: the rename-aside/backup path is back in the capture loop — that is the nesting hazard, and it was deleted rather than hardened (#3312 job 10, blocker 1)'
    else
      ok 'structural: no rename-aside or backup path exists in the capture loop (deleted, not hardened)'
    fi
    if printf '%s\n' "$_cap_body" | grep -qF '_roborev_file_digest'; then
      ok 'structural: reuse is keyed on a content digest, not on the byte count'
    else
      bad 'structural: the capture loop has no digest check — a same-length rewrite would keep a stale capture (#3312 job 7, finding 3)'
    fi
    # THREE DIGESTS, THREE VARIABLES, AND AN EXPLICIT EMPTINESS REFUSAL (job 8). The helper returns
    # through ONE global, so re-reading it into the same name is how the post-copy check came to compare
    # a value with itself; and an empty digest compares equal to another empty one, so "unmeasured" must
    # be refused by name rather than assumed impossible.
    _dg_missing=""
    for _dgv in dg_pre dg_post dg_staged; do
      printf '%s\n' "$_cap_body" | grep -qF "$_dgv=" || _dg_missing="$_dg_missing $_dgv"
      printf '%s\n' "$_cap_body" | grep -qF "[ -z \"\$$_dgv\" ]" || _dg_missing="$_dg_missing ${_dgv}-emptiness"
    done
    printf '%s\n' "$_cap_body" | grep -qF '[ "$dg_pre" != "$dg_post" ]' || _dg_missing="$_dg_missing pre-vs-post"
    printf '%s\n' "$_cap_body" | grep -qF '[ "$dg_pre" != "$dg_staged" ]' || _dg_missing="$_dg_missing pre-vs-staged"
    printf '%s\n' "$_cap_body" | grep -qF 'digest=%s\\n' >/dev/null 2>&1 || true
    printf '%s\n' "$_cap_body" | grep -qF '"$rel" "$dg_pre"' || _dg_missing="$_dg_missing meta-stamped-with-agreed-digest"
    if [ -z "$_dg_missing" ]; then
      ok 'structural: publication requires pre/post/staged digests that are each MEASURED and all AGREE'
    else
      bad "structural: the three-way digest agreement is incomplete —$_dg_missing. A digest re-read into the same variable compares with itself, and an empty one compares equal to another empty one (#3312 job 8)"
    fi
    if printf '%s\n' "$_cap_body" | grep -qF '_rx_capture_stop'; then
      ok 'structural: the watcher shuts down gracefully (TERM asks; the in-flight iteration completes)'
    else
      bad 'structural: the watcher has no graceful-shutdown flag — a kill mid-capture would abandon a good snapshot (#3312 job 7, finding 2)'
    fi
  fi
fi
# The validator must refuse symlinks at the FILE and at the components it descends — parent-component
# symlinks are the half that a `-L` on the file alone misses.
if [ -n "${_cap_val_defs:-}" ] && [ "${_cap_val_defs:-0}" -eq 1 ]; then
  _cvs=$(grep -nE '^_roborev_capture_validate\(\) \{' "$ORACLES" | head -1 | cut -d: -f1)
  _cve=$(awk -v s="$_cvs" 'NR>s && /^}/ {print NR; exit}' "$ORACLES")
  _cv_body=$(sed -n "${_cvs},${_cve:-$_cvs}p" "$ORACLES")
  _cv_l_tests=$(printf '%s\n' "$_cv_body" | grep -cE '\[ ! -L ' || true)
  if [ "${_cv_l_tests:-0}" -ge 3 ]; then
    ok "structural: the capture validator refuses symlinks at the file AND at each component it descends ($_cv_l_tests -L tests)"
  else
    bad "structural: the capture validator carries only ${_cv_l_tests:-0} symlink test(s) — a symlinked .roborev or snapshot directory would still be followed (#3312 blocker 1)"
  fi
  if printf '%s\n' "$_cv_body" | grep -qF 'pwd -P'; then
    ok 'structural: the capture validator resolves the containing directory physically before trusting it'
  else
    bad 'structural: the capture validator does not physically resolve the snapshot directory (#3312 blocker 1)'
  fi
fi
# The consumer must REQUIRE the record and compare the WHOLE relative path, never the dir id alone.
if [ -n "${_src_defs:-}" ] && [ "${_src_defs:-0}" -eq 1 ]; then
  _rs=$(grep -nE '^roborev_collect_review_diff_headers\(\) \{' "$ORACLES" | head -1 | cut -d: -f1)
  _re=$(awk -v s="$_rs" 'NR>s && /^}/ {print NR; exit}' "$ORACLES")
  _r_body=$(sed -n "${_rs},${_re:-$_rs}p" "$ORACLES")
  if printf '%s\n' "$_r_body" | grep -qF 'capture-unidentified' \
    && printf '%s\n' "$_r_body" | grep -qF 'capture-path-mismatch'; then
    ok 'structural: the resolver has both capture-identity refusals (unidentified, path-mismatch)'
  else
    bad 'structural: the resolver is missing a capture-identity refusal — an unidentified or mis-keyed capture could be read as evidence (#3312)'
  fi
  # THE FALLBACK PREDICATE IS NON-EXISTENCE, NOT "not a regular file" (job 10, blocker 2): a directory
  # or FIFO at the named path is "unreadable", a fact to REPORT, not "the snapshot is gone".
  if printf '%s\n' "$_r_body" | grep -qF 'if [ ! -e "$_rx_snap_bound_path" ] && [ -n "${ROBOREV_SNAPSHOT_CAPTURE_DIR:-}" ]'; then
    ok 'structural: a capture is consulted only when the live path does NOT EXIST'
  else
    bad 'structural: the capture fallback is not keyed on non-existence — an unreadable non-regular file at the named path would be silently answered from a capture (#3312 job 10, blocker 2)'
  fi
  if printf '%s\n' "$_r_body" | grep -qE '\[ "\$_rx_cap_recorded" = "\$\{_rx_snap_rel:-\}" \]'; then
    ok 'structural: a capture is accepted only when its recorded path EQUALS the path the prompt names'
  else
    bad 'structural: the capture lookup no longer compares the recorded path with the prompt-named path — the canonical sibling could stand in for another file in the same snapshot dir (#3312 blocker 2)'
  fi
fi

# THE ESCALATION STAYS GUARDED (job 11). Its expected failure mode is "the watcher already exited", and
# an unguarded `kill -KILL` as the last command of an AND-OR list aborts the wrapper under `set -e`.
_stop_s=$(grep -nE '^roborev_snapshot_capture_stop\(\) \{' "$ORACLES" | head -1 | cut -d: -f1)
_stop_e=""
[ -z "$_stop_s" ] || _stop_e=$(awk -v s="$_stop_s" 'NR>s && /^}/ {print NR; exit}' "$ORACLES")
if [ -z "$_stop_s" ] || [ -z "$_stop_e" ]; then
  bad 'structural: could not locate roborev_snapshot_capture_stop to inspect its escalation'
else
  _stop_body=$(sed -n "${_stop_s},${_stop_e}p" "$ORACLES")
  if printf '%s\n' "$_stop_body" | grep -qE 'kill -0 [^&]*&& kill -KILL'; then
    bad 'structural: the KILL escalation is back to an unguarded AND-OR list — an already-exited watcher aborts the wrapper under set -e (#3312 job 11)'
  elif printf '%s\n' "$_stop_body" | grep -qE 'kill -KILL "\$ROBOREV_SNAPSHOT_CAPTURE_PID" 2>/dev/null \|\| :'; then
    ok 'structural: the KILL escalation tolerates an already-exited watcher (no spurious wrapper abort)'
  else
    bad 'structural: the KILL escalation is neither guarded nor absent — check roborev_snapshot_capture_stop (#3312 job 11)'
  fi
fi

# THE AFFIRMATIVE-MEASUREMENT SHAPE, at the new branch point (CLAUDE.md; #3229 round-10). The
# diff-source state is MULTI-VALUED, which is precisely the shape whose unmeasured states inherit
# the permissive branch when only the bad ones are tested. So prompt-content: must select on the
# AFFIRMATIVE state names and fail closed in its `*)` arm.
_pc_start=$(grep -nE '^roborev_check_prompt_content\(\) \{' "$CHECKS_FILE" | head -1 | cut -d: -f1)
if [ -z "$_pc_start" ]; then
  bad 'structural: roborev_check_prompt_content is not defined'
else
  _pc_end=$(awk -v s="$_pc_start" 'NR>s && /^}/ {print NR; exit}' "$CHECKS_FILE")
  if [ -z "$_pc_end" ] || [ "$_pc_end" -le "$_pc_start" ]; then
    bad "structural: the prompt-content body bounds could not be resolved (start $_pc_start, end '${_pc_end:-<none>}')"
  else
    _pc_body=$(sed -n "${_pc_start},${_pc_end}p" "$CHECKS_FILE")
    _pc_arm_missing=""
    printf '%s\n' "$_pc_body" | grep -qF 'case "${ROBOREV_DIFF_SOURCE_STATE:-}" in' \
      || _pc_arm_missing="$_pc_arm_missing the-state-switch"
    for _st in inline snapshot both none; do
      printf '%s\n' "$_pc_body" | grep -qE "^[[:space:]]*$_st\)" || _pc_arm_missing="$_pc_arm_missing $_st"
    done
    # The catch-all arm is what makes the allow-list CLOSED: without it every unmeasured state
    # falls through to the census comparison as though a source had been read.
    printf '%s\n' "$_pc_body" | grep -qE '^[[:space:]]*\*\)' || _pc_arm_missing="$_pc_arm_missing *)-fail-closed"
    if [ -z "$_pc_arm_missing" ]; then
      ok 'structural: prompt-content selects the diff source on its AFFIRMATIVE state names, with a closed catch-all'
    else
      bad "structural: prompt-content does not select the diff source on an affirmative allow-list of states — missing:$_pc_arm_missing. An unmeasured state would inherit the permissive branch (#3312)"
    fi
    if printf '%s\n' "$_pc_body" | grep -qF 'snapshot diff unusable'; then
      ok 'structural: an unrecognised or failed diff-source state reaches a FAIL value under prompt-content:'
    else
      bad 'structural: prompt-content has no fail-closed arm for a failed/unrecognised diff-source state (#3312)'
    fi
  fi
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
_em_start=$(grep -nE '^emit_summary\(\) \{' "$WRAPPER" | head -1 | cut -d: -f1)
_em_end=""
[ -z "$_em_start" ] || _em_end=$(awk -v s="$_em_start" 'NR>s && /^}/ {print NR; exit}' "$WRAPPER")
if [ -z "$_em_start" ] || [ -z "$_em_end" ]; then
  bad 'structural: could not locate the emit_summary() body to inspect'
else
  # Every executable line in the body is either the block BANNER or an `emit_kv` call.
  _em_raw=$(sed -n "$((_em_start + 1)),$((_em_end - 1))p" "$WRAPPER" \
    | grep -vE '^[[:space:]]*(#|$)' \
    | grep -vE "^[[:space:]]*emit_kv '" \
    | grep -vF "printf '==== ROBOREV REVIEW SUMMARY ====" || true)
  if [ -z "$_em_raw" ]; then
    ok "structural: every emit_summary value goes through emit_kv (lines $_em_start-$_em_end)"
  else
    bad "structural: emit_summary emits a value WITHOUT emit_kv, so a newline-bearing path could forge a key: ${_em_raw%%$'\n'*}"
  fi
  # 22 emit_kv lines = 21 keys + the terminal `RESULT:`, which goes through the SAME
  # neutralising boundary. Was 23 before #3229's owner ruling removed `census-exclusion:`
  # with its oracle (#3283).
  _em_n=$(sed -n "$((_em_start + 1)),$((_em_end - 1))p" "$WRAPPER" | grep -cE "^[[:space:]]*emit_kv '" || true)
  if [ "${_em_n:-0}" -ge 22 ]; then
    ok "structural: all $_em_n block lines (21 keys + RESULT:) are emitted through the neutralising boundary"
  else
    bad "structural: only ${_em_n:-0} emit_kv call(s) in emit_summary — the block has 21 keys plus RESULT:, so some are emitted another way"
  fi
fi
if grep -qE '^[[:space:]]*roborev_safe_line "\$2"' "$WRAPPER"; then
  ok 'structural: emit_kv neutralises its value before printing it'
else
  bad 'structural: emit_kv does not call roborev_safe_line — the boundary is decorative'
fi
# DETAILS reach the SAME stdout a reader greps for `^RESULT: `, so the bulk
# `printf '%s\n' "${DETAILS[@]}"` (which prints a newline-bearing entry as several lines)
# must be gone, replaced by a per-entry neutralised print.
if grep -qE 'printf .%s..n. "\$\{DETAILS\[@\]\}"' "$WRAPPER"; then
  bad 'structural: finish still bulk-prints "${DETAILS[@]}" — a newline-bearing DETAILS entry would span lines and could forge a RESULT: line'
else
  ok 'structural: DETAILS are not bulk-printed (each entry is neutralised individually)'
fi
_fin_start=$(grep -nE '^finish\(\) \{' "$WRAPPER" | head -1 | cut -d: -f1)
_fin_end=""
[ -z "$_fin_start" ] || _fin_end=$(awk -v s="$_fin_start" 'NR>s && /^}/ {print NR; exit}' "$WRAPPER")
if [ -n "$_fin_start" ] && [ -n "$_fin_end" ] \
  && sed -n "${_fin_start},${_fin_end}p" "$WRAPPER" | grep -q 'roborev_safe_line'; then
  ok "structural: finish neutralises every DETAILS line (lines $_fin_start-$_fin_end)"
else
  bad 'structural: finish does not neutralise DETAILS lines'
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
_scan_start=$(grep -nE '^[[:space:]]*for verdict in ' "$WRAPPER" | head -1 | cut -d: -f1 || printf '')
_scan_end=''
if [ -n "$_scan_start" ]; then
  _scan_end=$(awk -v s="$_scan_start" 'NR>s && /^[[:space:]]*done[[:space:]]*$/ {print NR; exit}' "$WRAPPER")
fi
_scan_block=''
_scan_keys=''
_scan_case=''
if [ -n "$_scan_start" ] && [ -n "$_scan_end" ]; then
  _scan_block=$(sed -n "${_scan_start},${_scan_end}p" "$WRAPPER")
  # The key list ends at the first line that does not continue with a trailing backslash.
  _scan_keys=$(printf '%s\n' "$_scan_block" | awk '{print} !/\\$/{exit}')
  _scan_case=$(printf '%s\n' "$_scan_block" | grep -E 'case "\$verdict_token" in' | head -1 || printf '')
fi
if [ -z "$_scan_block" ] || [ -z "$_scan_keys" ] || [ -z "$_scan_case" ]; then
  bad 'structural: could not locate the wrapper verdict scan STATEMENT (for verdict in … case … done) to inspect'
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
    | grep -E 'PASS\|SKIP\|NOTICE\|UNAVAILABLE\|DEGRADED' | head -1 || printf '')
  if [ -n "$_scan_positive" ]; then
    ok 'structural: the verdict scan has a POSITIVE arm — the non-failing set is an allow-list, not "not-failing"'
  else
    bad 'structural: the verdict scan has NO positive arm, so any value outside FAIL*|FINDINGS*|ERROR*|INCONSISTENT* — an empty string, a state a future check invents — inherits the non-failing branch and reaches finish PASS (#3229 round-10)'
  fi
  # The `*)` fallback must SET failed=1. Read from the positive `case`'s own body: from the
  # allow-list line to the `esac` that closes it.
  _scan_fallthrough=$(printf '%s\n' "$_scan_block" \
    | awk '/PASS\|SKIP\|NOTICE/ { inb = 1 } inb { print } inb && /esac/ { exit }' \
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
  if grep -qE '^[[:space:]]*not_affirmed="\$\{not_affirmed' "$WRAPPER" &&
    grep -qE '^[[:space:]]*PASS\) continue ;;' "$WRAPPER" &&
    grep -qE '^[[:space:]]*case "\$\{det_value%% \*\}" in' "$WRAPPER"; then
    ok 'structural: a PASS requires each verdict-carrying key to be affirmatively PASS, matched on the exact token (the SKIP backstop)'
    _aff_missing=""
    for _aff_key in push-assert census-check code-free sha-assert \
      review-completed prompt-content; do
      grep -qE "\"$_aff_key=\\\$[A-Z_]+\"" "$WRAPPER" || _aff_missing="$_aff_missing $_aff_key"
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
  _aff_body=$(awk '/for keyed in "push-assert=/ { inb = 1 } inb { print } inb && /^[[:space:]]*esac[[:space:]]*$/ { exit }' "$WRAPPER")
  if [ -z "$_aff_body" ]; then
    bad 'structural: could not locate the affirmation backstop case body to inspect for per-key exemptions'
  else
    _aff_continues=$(printf '%s\n' "$_aff_body" | grep -cE '\bcontinue\b' || true)
    if [ "$_aff_continues" -eq 1 ] &&
      printf '%s\n' "$_aff_body" | grep -qE '^[[:space:]]*PASS\) continue ;;'; then
      ok 'structural: the affirmation backstop has exactly ONE exempting arm, the affirmative PASS) one — no per-key escape hatch'
    else
      bad "structural: the affirmation backstop carries $_aff_continues exempting arm(s); a non-PASS value is exempted for some key, so that key can reach finish PASS on a non-measurement (#3229 owner ruling / #3283)"
    fi
  fi
  # And the wrapper must STATE the rule, not just implement it: the next key added to this
  # block is written by someone reading the doc block, not the scan.
  if grep -qF 'A POSITIVE VERDICT REQUIRES AN AFFIRMATIVE MEASUREMENT' "$WRAPPER"; then
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
  if grep -qE "emit_kv 'census-exclusion'" "$WRAPPER"; then
    bad 'structural: the summary block still emits a census-exclusion key whose oracle is deleted'
  else
    ok 'structural: the summary block no longer emits census-exclusion (removal visible in the OUTPUT contract, not just the source)'
  fi
  for _gone_fn in roborev_check_census_exclusion roborev_format_exclude_args \
    roborev_toml_exclude_patterns roborev_parse_toml_array roborev_corroborate_exclude_patterns; do
    if grep -qE "(^|[^#[:alnum:]_])$_gone_fn\b" "$WRAPPER" "$SCRIPT_DIR/../flow/roborev-review-oracles.sh" \
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
