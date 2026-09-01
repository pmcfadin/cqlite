#!/usr/bin/env bash
# shellcheck shell=bash
# claude-auth-capability.sh — THE one place that knows whether a fleet box can start
# `claude` on a COLD, non-interactive lane, and how that is VERIFIED (issue #3733).
#
# THE FIELD FAILURE. A newly created tmux session lands on claude's first-run login
# chooser, so a retired lane cannot be replaced. The causal chain was measured end to
# end on the box; treat these as established facts, not conjecture:
#   1. the ONLY working credential is the env var CLAUDE_CODE_OAUTH_TOKEN.
#      $CLAUDE_CONFIG_DIR/.credentials.json holds EMPTY accessToken/refreshToken and
#      `expiresAt: 0` — it authenticates nothing;
#   2. the token authenticates INDEPENDENTLY of CLAUDE_CONFIG_DIR (token + a fresh empty
#      config dir authenticates). The config dir is onboarding/session state, not auth;
#   3. the token is provisioned in /etc/environment ONLY, which is read by pam_env — so
#      it reaches login/ssh sessions and nothing else. /etc/profile.d/30-agent-ami-data.sh
#      carries CLAUDE_CONFIG_DIR but NOT the token;
#   4. a tmux pane's environment comes from the tmux SERVER, fixed at server start. A
#      server that predates provisioning yields panes with NEITHER variable. This is the
#      actual field failure;
#   5. `tmux new-session <command>` does not run the command through a login shell, so
#      /etc/profile.d never executes for a spawned lane either;
#   6. therefore NOTHING ON DISK distinguishes a working box from a broken one — the
#      distinguishing state is a long-running process's start environment, which is why
#      the failure is silent until dispatch.
#
# HENCE TWO INDEPENDENT VERDICTS, each on its own greppable line, because they fail
# independently and their remedies differ:
#
#   claude-auth:      is the PERSISTED credential valid for a cold, non-interactive start?
#                     VERIFIED | NOT-PERSISTED | FAILED | UNMEASURED
#   claude-tmux-env:  does that credential REACH a tmux-spawned pane?
#                     live server:  VERIFIED | SERVER-STALE | SERVER-MISSING |
#                                   SERVER-INCOMPLETE | SERVER-CONFIG-STALE |
#                                   SERVER-CONFIG-NODIR
#                     no server:    VERIFIED | COLD-START-MISSING |
#                                   COLD-START-INCOMPLETE | COLD-START-NODIR
#                     either:       NO-SERVER | UNMEASURED
#
# A SERVERLESS BOX IS NOT A DEAD END. That is the NORMAL state of a freshly provisioned
# machine at the exact moment `.agent-ami/profile.yaml` runs bootstrap with `--strict`, so a
# blanket non-pass there reds this check on its PRIMARY use case with no way out — the guard
# agents learn to waive. The answerable question is then not "what does the live server
# hold" but "would a NEWLY created server deliver the credential to a pane", and it is
# measured directly: an isolated throwaway tmux server on a PRIVATE socket, started from the
# PERSISTED environment, spawns one pane and reports what it received. `NO-SERVER` now means
# only that THAT probe could not run, and it stays UNMEASURED-class. The COLD-START-* names
# are deliberately distinct from the SERVER-* ones — a live server that lacks the credential
# and a would-be server that would lack it are different operator actions.
#
# NONEMPTY IS NOT CORRECT. VERIFIED requires the server's CLAUDE_CONFIG_DIR to EQUAL the
# persisted value AND that directory to EXIST — testing only "is it absent" and letting
# every other state fall through to VERIFIED is the two-valued predicate that always picks
# the permissive answer, and a wrong config dir is the un-onboarded first-run picker this
# file exists to catch. DECLARED RESIDUAL: `exists` is not `onboarded`. Whether a config
# directory holds usable onboarding state is deliberately NOT probed — it would mean
# depending on an internal JSON field shape that can change upstream, and a check that
# silently stops matching reports VERIFIED for the wrong reason.
#
# ONLY `VERIFIED` IS A SUCCESS, on either line. NO-SERVER is UNMEASURED-class: the isolated
# cold-start probe could not run, so nothing was measured (it does NOT merely mean "no
# server was running" — that case is measured now). An unmeasured capability never inherits the
# permissive branch — the standing rule that a positive verdict requires an AFFIRMATIVE
# MEASUREMENT (docs/development/fleet-runbook.md; CLAUDE.md, "git-push:"/"gate-pin:").
#
# THE SCRUB IS LOAD-BEARING, and it is #3414's central lesson one subject over. Bootstrap
# runs inside a session that ALREADY carries CLAUDE_CODE_OAUTH_TOKEN, so an unscrubbed
# check answers about the INHERITED value while claiming to answer about the PERSISTED
# one — which would certify the exact box this file exists to catch. The persisted value
# is read from the pam_env FILE, never from this process's environment, and the probe is
# handed that value explicitly with the inherited one removed. BASH_ENV/ENV are scrubbed
# with it: a non-interactive bash SOURCES $BASH_ENV, and that file can re-export what was
# just scrubbed, so scrubbing the variable while leaving the mechanism that re-injects it
# is not a scrub (#3414 hit exactly this).
#
# WHICH `-u` IS THE MECHANISM AND WHICH IS BELT — measured by deleting each one and running
# scripts/tests/test_claude_auth_capability.sh, because a scrub nothing can falsify is a
# scrub nothing asserts:
#   * `-u BASH_ENV` in the `--auth` probe is THE MECHANISM. Remove it and a $BASH_ENV file
#     re-exporting the credential reaches the child AFTER `env KEY=<persisted>` has run and
#     overrides it — a VERIFIED that is about the inherited value. Pinned by case 2b.
#   * `-u $CLAUDE_AUTH_TOKEN_KEY` in the COLD PROBE is THE MECHANISM, because the
#     re-supplying assignment there is CONDITIONAL (`[ -z "$__ptok" ] ||`): with nothing
#     persisted, that flag is the only thing keeping the inherited token out of the pane.
#     Pinned by the cold-start scrub case in section 21.
#   * `-u $CLAUDE_AUTH_TOKEN_KEY` in the `--auth` probe is BELT, redundant BY CONSTRUCTION:
#     `KEY=$__tok` always follows on the same `env` and an explicit assignment always wins,
#     and the function returns early when the persisted value is empty. No test can red on
#     its removal. Kept because over-scrubbing costs one word; DECLARED here rather than
#     covered by a case that would be asserting something already true.
#   * `-u ENV` is belt everywhere: $ENV is read only by an interactive POSIX shell (bash
#     only in posix mode), and every shell started here — the `claude` child, the cold
#     probe's `sh -c` pane — is non-interactive.
#
# THE TOKEN VALUE IS NEVER PRINTED — not to stdout, not to a log, not into a diagnostic.
# Everything reports SET/ABSENT/MATCH/DIFFERS. Comparison is by string equality on the
# extracted VALUE of both sides (never on a reconstructed `KEY=value` line: extracting the
# two sides differently is what produced a false finding on this issue's own thread).
# THE BOUNDARY, STATED AS WHAT THE CODE DOES: the FINAL rendering of every verdict detail
# passes through `claude_auth_redact` at its RENDER site — `claude_auth_emit_auth` /
# `claude_auth_emit_tmux` here, and the two report lines in bootstrap's section 5c — so a
# probe that quotes the credential back at us cannot relay it into a verdict line. The
# extra `claude_auth_redact` calls WITHIN a few detail strings (probe output, tmux stderr)
# are BELT on top of that, not the boundary; an earlier version of this paragraph claimed a
# single call site and there were five, while bootstrap — the primary consumer — had none.
#
# PLATFORM. /etc/environment + pam_env is Linux-specific. On a non-Linux host BOTH lines
# are UNMEASURED and NEVER an [ok]: scoping a platform out is not the same as passing it,
# and an [ok] is what `--strict` reads (a real #3414 defect).
#
# TEST-ONLY ENV SEAM — INERT UNLESS CQLITE_BOOTSTRAP_TEST_MODE=1:
#   CQLITE_CLAUDE_AUTH_ENV_FILE   stand-in for /etc/environment (test mode only)
# A seam set WITHOUT the marker is a LOUD REFUSAL (UNMEASURED), never a silent honouring
# and never a silent ignore: this file's whole subject is a verdict that can be wrong
# about which environment it measured.
#
# Sourced by scripts/bootstrap-agent-machine.sh; also runnable standalone (see --help).

# ---- production location: HARDCODED. Never env-derived outside test mode. ----
CLAUDE_AUTH_ENV_FILE_DEFAULT='/etc/environment'
CLAUDE_AUTH_TOKEN_KEY='CLAUDE_CODE_OAUTH_TOKEN'
CLAUDE_AUTH_CONFIG_KEY='CLAUDE_CONFIG_DIR'
# The probe's success marker. Requiring it ALONGSIDE rc 0 is what makes the probe
# affirmative: `claude -p` can exit 0 having produced nothing, and rc alone cannot tell
# "authenticated" from "authenticated and returned nothing".
#
# THE SENTINEL IS DELIBERATELY ABSENT FROM THE PROMPT. It used to BE the prompt's last word
# ("Reply with exactly this word …: <SENTINEL>"), so `grep -qF "$SENTINEL"` could not
# distinguish an ANSWER from an ECHO OF THE INPUT — anything that prints its own argv
# passed, and this repo shipped exactly that stub. Asking for a TRANSFORMATION the prompt
# does not contain (the uppercase form) means the expected string exists nowhere in the
# input, so an echo cannot satisfy it while any model that read the prompt can. Keep the two
# constants consistent: the prompt must never contain CLAUDE_AUTH_SENTINEL verbatim.
CLAUDE_AUTH_SENTINEL='CQLITE_CLAUDE_AUTH_OK'
CLAUDE_AUTH_PROMPT='Reply with the UPPERCASE form of the following word, and nothing else: cqlite_claude_auth_ok'
CLAUDE_AUTH_PROBE_BOUND=90
# The cold-start tmux probe is local-only (no network), so it gets a much tighter bound.
CLAUDE_AUTH_TMUX_PROBE_BOUND=20

claude_auth_test_mode() { [ "${CQLITE_BOOTSTRAP_TEST_MODE:-}" = 1 ]; }
claude_auth_seam_set()  { [ -n "${CQLITE_CLAUDE_AUTH_ENV_FILE:-}" ]; }

# claude_auth_platform_linux: rc 0 iff this host has the pam_env mechanism at all.
claude_auth_platform_linux() { [ "$(uname -s 2>/dev/null)" = Linux ]; }

# ---- bounded execution -------------------------------------------------------------
# Resolved by PROBING the candidate, not by trusting its name: GNU coreutils installs its
# timeout as `gtimeout` on macOS, and a BusyBox `timeout` REJECTS --kill-after. Same
# resolution bootstrap uses, duplicated here only because this file must also run
# standalone. A candidate that supports --kill-after wins even if it is second.
CLAUDE_AUTH_TIMEOUT_BIN=""
CLAUDE_AUTH_TIMEOUT_KILL=0
claude_auth_resolve_timeout() {
  local name path
  CLAUDE_AUTH_TIMEOUT_BIN=""; CLAUDE_AUTH_TIMEOUT_KILL=0
  for name in timeout gtimeout; do
    path="$(command -v "$name" 2>/dev/null || true)"
    [ -n "$path" ] || continue
    if "$path" --kill-after=1 1 true >/dev/null 2>&1; then
      CLAUDE_AUTH_TIMEOUT_BIN="$path"; CLAUDE_AUTH_TIMEOUT_KILL=1; return 0
    fi
    [ -n "$CLAUDE_AUTH_TIMEOUT_BIN" ] || CLAUDE_AUTH_TIMEOUT_BIN="$path"
  done
  [ -n "$CLAUDE_AUTH_TIMEOUT_BIN" ]
}

# ---- matching: BUILTIN ONLY, NEVER THROUGH A PIPE -----------------------------------
# `set -o pipefail` is on and `grep -q`/`grep -m1` EXIT ON THE FIRST MATCH, so when the
# producer still has more than a pipe buffer (64 KiB on Linux) to write it takes SIGPIPE
# and dies 141 — and the PIPELINE reports failure for a SUCCESSFUL match. Measured on this
# box: a 200 KB blob whose needle is on line 1 gives `printf … | grep -qF …` rc 141. Both
# consequences here are wrong in the dangerous direction: a valid credential reads as
# unmeasured, and a correctly seeded tmux server reads as SERVER-MISSING — whose remedy is
# to overwrite the value that is already right. `tmux show-environment -g` on a heavily
# populated server is an ordinary producer of that much output.
#
# The fix is to REMOVE THE PIPE rather than to widen the buffer or add `|| true` (which
# would swallow a real grep ERROR — the three-valued-rc trap this file already avoids
# elsewhere). Everything below is a bash builtin: no second process, so no race to lose.

# claude_auth_contains <haystack> <needle>: literal substring test.
claude_auth_contains() {
  case "$1" in *"$2"*) return 0 ;; esac
  return 1
}

# claude_auth_matches_ci <text> <ere>: case-insensitive extended-regex test. `nocasematch`
# is a SHELL-WIDE option and this file is SOURCED by bootstrap, so its previous state is
# saved and restored — turning an option on for a caller that did not ask for it is the
# same class as replacing a caller's trap.
claude_auth_matches_ci() {
  local __prev=0 __rc=1
  shopt -q nocasematch && __prev=1
  shopt -s nocasematch
  # $2 is deliberately UNQUOTED: quoted, `=~` would compare it literally.
  [[ "$1" =~ $2 ]] && __rc=0
  [ "$__prev" = 1 ] || shopt -u nocasematch
  return "$__rc"
}

# ---- redaction: THE ONE EMIT BOUNDARY ----------------------------------------------
# claude_auth_redact <text>: <text> with the persisted credential replaced, control
# characters flattened, and the result truncated. Applied at the single place a detail is
# rendered rather than per interpolation site — a per-site escape is a list to keep
# complete, and this repo has paid for that twice (CLAUDE.md, roborev job 230).
# Over-redaction is the safe direction: the pattern is a glob, so a value containing a
# glob metacharacter can only remove MORE text, never less.
CLAUDE_AUTH_SECRET=''
claude_auth_redact() {
  local t="${1:-}"
  if [ -n "$CLAUDE_AUTH_SECRET" ]; then t="${t//"$CLAUDE_AUTH_SECRET"/<redacted>}"; fi
  t=$(printf '%s' "$t" | tr -d '\000' | tr '\n\r\t' '   ' | cut -c1-500)
  printf '%s' "$t"
}

# ---- the pam_env file --------------------------------------------------------------
# claude_auth_env_file_into <outvar>: the file whose contents pam_env consumes. rc 1 with
# a LOUD refusal when the seam is set without the marker.
claude_auth_env_file_into() {
  eval "$1="
  if claude_auth_seam_set; then
    if ! claude_auth_test_mode; then
      printf 'claude-auth-capability: REFUSING: CQLITE_CLAUDE_AUTH_ENV_FILE is a TEST-ONLY seam (inert without CQLITE_BOOTSTRAP_TEST_MODE=1). Unset it; the production path is %s.\n' \
        "$CLAUDE_AUTH_ENV_FILE_DEFAULT" >&2
      return 1
    fi
    eval "$1=\$CQLITE_CLAUDE_AUTH_ENV_FILE"
    return 0
  fi
  eval "$1=\$CLAUDE_AUTH_ENV_FILE_DEFAULT"
}

# claude_auth_strip_pam_quotes_into <outvar> <raw>: reproduce pam_env's own de-quoting so
# the value compared is the value a session actually RECEIVES. Measured on this fleet's
# pam (1.5.3): drop a LEADING `"` or `'` whether or not it is closed, then drop a trailing
# one of the SAME kind if present. Stripping quotes is reading the file's FORMAT;
# normalising the value would be reinterpreting it, and is forbidden.
claude_auth_strip_pam_quotes_into() {
  local __v="${2:-}" __q
  case "$__v" in
    '"'*) __q='"' ;;
    "'"*) __q="'" ;;
    *) eval "$1=\$__v"; return 0 ;;
  esac
  __v=${__v#?}
  case "$__v" in *"$__q") __v=${__v%?} ;; esac
  eval "$1=\$__v"
}

# claude_auth_read_key_into <outvar_value> <outvar_state> <file> <key>:
# parse ONE assignment the way pam_env reads the file. THE GRAMMAR IS MEASURED, not
# reasoned about: /etc/pam.d/sudo carries `pam_env.so readenv=1`, so appending a probe line
# to /etc/environment and reading `sudo env` shows exactly what pam_env delivered. On this
# fleet (pam 1.5.3):
#   * leading whitespace is skipped, then an EXACT 7-byte `export ` prefix is dropped
#     (delivered: `export K=v`, `  export K=v`; NOT delivered: `export  K=v` with two
#     spaces, `export<TAB>K=v`, `exportK=v`, `setenv K=v`). `man 8 pam_env` documents it —
#     "The export instruction can be specified for bash compatibility, but will be ignored"
#     — and `export ` is the one such literal in the shipped pam_env.so;
#   * the key runs up to the FIRST `=` with NO whitespace before it: `K = v` delivered
#     nothing usable, so a `K[[:space:]]*=` anchor would report PERSISTED for a line no
#     session receives — the permissive direction, and the worse one;
#   * whole-line `#` comments are skipped (the anchor forbids a leading `#`);
#   * NO inline-comment stripping: pam_env takes a trailing `# ...` as part of the value;
#   * the LAST assignment wins if a file somehow carries two.
# States: present | absent | absent-file | unreadable | unparseable.
# A SYMLINK is `unreadable`, not followed: what it points at is not the file whose bytes
# pam_env consumes, and an unknown must never resolve to the good case.
claude_auth_read_key_into() {
  local __ov="$1" __os="$2" __f="$3" __k="$4" __raw __g=0
  eval "$__ov="; eval "$__os=unreadable"
  # `-L` IS TESTED FIRST, and the order is the whole point: a DANGLING symlink fails `-e`,
  # so an existence test in front of it answered `absent-file` — "nothing is provisioned
  # here, add a line" — about a path we deliberately refuse to look through. The comment
  # above said symlinks are refused; the code said they are missing. Refusal wins.
  if [ -L "$__f" ]; then eval "$__os=unreadable"; return 0; fi
  if [ ! -e "$__f" ]; then eval "$__os=absent-file"; return 0; fi
  if [ ! -f "$__f" ] || [ ! -r "$__f" ]; then eval "$__os=unreadable"; return 0; fi
  # `grep` IS THREE-VALUED: 0 match, 1 NO match, >=2 ERROR (127 absent). `if ! grep -q`
  # reads it two-valued and collapses "cannot tell" onto the AFFIRMATIVE `absent`, which
  # reported NOT-PERSISTED for a box whose token IS provisioned and sent the operator to
  # add a line already there. This repo lints for that shape (`1699-find-tristate`), and
  # `unreadable` is already in this function's own state set. NOT A PIPE: `$?` on its own
  # line.
  grep -Eq "^[[:space:]]*(export )?$__k=" "$__f" 2>/dev/null
  __g=$?
  case "$__g" in
    0) ;;
    1) eval "$__os=absent"; return 0 ;;
    *) eval "$__os=unreadable"; return 0 ;;
  esac
  # A SENTINEL PREFIX, because an EMPTY capture and a FAILED capture are otherwise the
  # same string: `KEY=` is a legitimate (empty) assignment while sed producing nothing
  # means the parse failed. Without the marker the `unparseable` branch is unreachable.
  __raw=$(sed -n "s/^[[:space:]]*\(export \)\{0,1\}$__k=/VAL:/p" "$__f" 2>/dev/null | tail -1)
  case "$__raw" in
    VAL:*) claude_auth_strip_pam_quotes_into "$__ov" "${__raw#VAL:}"; eval "$__os=present" ;;
    *)     eval "$__os=unparseable" ;;
  esac
}

# ---- (a) claude-auth: is the PERSISTED credential valid for a cold start? ----------
# claude_auth_verdict_into <outvar_verdict> <outvar_detail> [<outvar_probe_output>]
#
# THE PROBE IS AFFIRMATIVE AND ISOLATED. It runs `claude -p` with a sentinel prompt under
# a hard bound and requires BOTH rc 0 AND the sentinel in the output. It runs against a
# FRESH, EMPTY CLAUDE_CONFIG_DIR, because the question is whether THE PERSISTED TOKEN
# authenticates — a probe inheriting the box's config dir could pass on a session state
# the token had nothing to do with, which is the inherited-answers-for-persisted shape
# again. Fact 2 above is what makes this safe: the token authenticates independently of
# the config dir.
#
# THE EXIT STATUS IS NEVER READ THROUGH A PIPE. `cmd | tail; echo $?` reports the
# PIPELINE'S LAST STAGE, and that trap inverted the reading of this very probe during
# diagnosis. Capture into a variable, then read `$?` on the next line.
claude_auth_verdict_into() {
  local __ov="$1" __od="$2" __op="${3:-}"
  local __file='' __state='' __tok='' __cfg='' __out='' __rc=0
  eval "$__ov=UNMEASURED"; eval "$__od="
  [ -z "$__op" ] || eval "$__op="

  if ! claude_auth_env_file_into __file; then
    eval "$__od='the TEST-ONLY seam CQLITE_CLAUDE_AUTH_ENV_FILE is set without CQLITE_BOOTSTRAP_TEST_MODE=1 — refusing to answer about an env-chosen file (details on stderr)'"
    return 0
  fi
  if ! claude_auth_platform_linux; then
    eval "$__od=\"/etc/environment + pam_env is a Linux mechanism; on \$(uname -s 2>/dev/null) there is no system-wide file a cold session would read, so the persisted credential cannot be measured\""
    return 0
  fi

  claude_auth_read_key_into __tok __state "$__file" "$CLAUDE_AUTH_TOKEN_KEY"
  CLAUDE_AUTH_SECRET="$__tok"
  case "$__state" in
    absent|absent-file)
      eval "$__ov=NOT-PERSISTED"
      eval "$__od=\"no \$CLAUDE_AUTH_TOKEN_KEY assignment in \$__file (\$__state) — a tmux-spawned lane inherits no credential at all and lands on the first-run login chooser\""
      return 0 ;;
    unreadable)
      eval "$__od=\"\$__file cannot be read as a regular file (a symlink, or no read permission), so what a PAM session would receive is UNKNOWN\""
      return 0 ;;
    unparseable)
      eval "$__od=\"\$__file carries a \$CLAUDE_AUTH_TOKEN_KEY line this parser could not read; a parse failure is an ABSENCE OF EVIDENCE, never a mismatch\""
      return 0 ;;
  esac
  if [ -z "$__tok" ]; then
    eval "$__ov=NOT-PERSISTED"
    eval "$__od=\"\$__file assigns \$CLAUDE_AUTH_TOKEN_KEY an EMPTY value — set but empty is a misconfigured box, not a provisioned one\""
    return 0
  fi

  if ! command -v claude >/dev/null 2>&1; then
    # NO BACKTICKS HERE, and none may come back: the OUTER quotes are double, so a backtick
    # (or an unescaped $(...)) anywhere inside — including inside this nested single-quoted
    # run — is LIVE and executes when the eval runs. The shipped form said `claude` and so
    # ran `claude`, emitting "claude: command not found" into the operator's transcript and
    # deleting the subject from the message. Pinned structurally by
    # scripts/tests/test_claude_auth_capability.sh.
    eval "$__od='no claude binary on PATH — the persisted credential exists but nothing here can exercise it'"
    return 0
  fi
  if ! claude_auth_resolve_timeout; then
    eval "$__od='no timeout/gtimeout on PATH — refusing to run an UNBOUNDED network probe (a wedged child would hang the provisioning entry point)'"
    return 0
  fi
  if ! __cfg=$(mktemp -d "${TMPDIR:-/tmp}/cqlite-claude-probe.XXXXXX") || [ ! -d "$__cfg" ]; then
    eval "$__od='could not create a throwaway CLAUDE_CONFIG_DIR for the probe, so the token could not be measured in isolation'"
    return 0
  fi
  # ARMED BETWEEN THE `mktemp -d` AND THE PROBE, for the same reason the cold probe arms
  # its own: an interrupt during a bounded, up-to-90s network call would otherwise leave a
  # `cqlite-claude-probe.*` directory behind on every SIGINT. The cold probe's machinery is
  # reused as-is — it kills a server only when a socket is registered, and none is here.
  CLAUDE_AUTH_PROBE_DIR="$__cfg"
  claude_auth_probe_arm_traps

  # NOT A PIPE. `__out=$(...)` then `__rc=$?` on its own line.
  if [ "$CLAUDE_AUTH_TIMEOUT_KILL" = 1 ]; then
    __out=$(env -u "$CLAUDE_AUTH_TOKEN_KEY" -u BASH_ENV -u ENV \
      "$CLAUDE_AUTH_TOKEN_KEY=$__tok" "$CLAUDE_AUTH_CONFIG_KEY=$__cfg" \
      "$CLAUDE_AUTH_TIMEOUT_BIN" --kill-after=5 "$CLAUDE_AUTH_PROBE_BOUND" \
      claude -p "$CLAUDE_AUTH_PROMPT" 2>&1)
  else
    __out=$(env -u "$CLAUDE_AUTH_TOKEN_KEY" -u BASH_ENV -u ENV \
      "$CLAUDE_AUTH_TOKEN_KEY=$__tok" "$CLAUDE_AUTH_CONFIG_KEY=$__cfg" \
      "$CLAUDE_AUTH_TIMEOUT_BIN" "$CLAUDE_AUTH_PROBE_BOUND" \
      claude -p "$CLAUDE_AUTH_PROMPT" 2>&1)
  fi
  __rc=$?
  claude_auth_probe_cleanup
  claude_auth_probe_restore_traps
  [ -z "$__op" ] || eval "$__op=\"\$(claude_auth_redact \"\$__out\")\""

  if [ "$__rc" -eq 0 ] && claude_auth_contains "$__out" "$CLAUDE_AUTH_SENTINEL"; then
    eval "$__ov=VERIFIED"
    eval "$__od=\"the \$CLAUDE_AUTH_TOKEN_KEY persisted in \$__file authenticated a cold, non-interactive 'claude -p' run against a FRESH empty config dir (rc 0 AND the sentinel returned)\""
    return 0
  fi
  if [ "$__rc" = 124 ] || [ "$__rc" = 137 ]; then
    eval "$__od=\"the probe exceeded its \${CLAUDE_AUTH_PROBE_BOUND}s bound and was killed — the credential is UNKNOWN, not ok\""
    return 0
  fi
  # NETWORK-UNREACHABLE IS UNMEASURED, NOT FAILED — but the matcher is deliberately narrow
  # and the fallback is the NON-PASSING one, so a shape it does not recognise degrades to
  # FAILED (a warn either way; only the operator's next action differs).
  if claude_auth_matches_ci "$__out" 'getaddrinfo|ENOTFOUND|ECONNREFUSED|EAI_AGAIN|network is unreachable|temporary failure in name resolution|connection error'; then
    eval "$__od=\"the probe could not reach the API (\$(claude_auth_redact \"\$__out\")) — the credential is UNKNOWN, not ok\""
    return 0
  fi
  eval "$__ov=FAILED"
  if [ "$__rc" -eq 0 ]; then
    eval "$__od=\"'claude -p' exited 0 but did NOT return the sentinel, so it did not answer: \$(claude_auth_redact \"\$__out\")\""
  else
    eval "$__od=\"the \$CLAUDE_AUTH_TOKEN_KEY persisted in \$__file did NOT authenticate (rc=\$__rc): \$(claude_auth_redact \"\$__out\")\""
  fi
}

# ---- (b) claude-tmux-env: does the credential REACH a tmux-spawned pane? -----------
# claude_tmux_env_verdict_into <outvar_verdict> <outvar_detail>
#
# THE DIMENSION THAT ACTUALLY FAILED IN THE FIELD. A pane's environment is the tmux
# SERVER's, fixed at server start, so a server predating provisioning hands out panes with
# neither variable however correct the disk is. A STALE server value is a distinct and
# worse state than none — the NOT-SYSTEM-WIDE analogue from #3414 — because everything
# looks provisioned and the credential is simply the wrong one.
claude_tmux_env_verdict_into() {
  local __ov="$1" __od="$2"
  local __file='' __state='' __tok='' __out='' __err='' __rc=0
  local __stok='' __sstate='' __scfg='' __scfgstate='' __cfg='' __cfgstate=''
  eval "$__ov=UNMEASURED"; eval "$__od="

  if ! claude_auth_env_file_into __file; then
    eval "$__od='the TEST-ONLY seam CQLITE_CLAUDE_AUTH_ENV_FILE is set without CQLITE_BOOTSTRAP_TEST_MODE=1 — refusing to answer about an env-chosen file (details on stderr)'"
    return 0
  fi
  # LINUX-SCOPED, and the reason is the BASELINE, not tmux (tmux runs on macOS fine): this
  # verdict is defined RELATIVE to the persisted /etc/environment + pam_env source, and
  # without that source there is nothing to compare a server's environment against. The
  # header block has always documented both lines as UNMEASURED off Linux; the guard was
  # missing here, so a macOS host could emit VERIFIED — and an [ok] is what `--strict`
  # reads (#3414: scoping a platform out is not the same as passing it).
  if ! claude_auth_platform_linux; then
    eval "$__od=\"/etc/environment + pam_env is a Linux mechanism; on \$(uname -s 2>/dev/null) there is no persisted baseline a tmux server could be compared against, so pane reachability cannot be measured\""
    return 0
  fi
  if ! command -v tmux >/dev/null 2>&1; then
    # No backticks: see the identical note in claude_auth_verdict_into. This one really did
    # run `tmux` and print "tmux: command not found" on a box with no tmux.
    eval "$__od='no tmux binary on PATH — there is no server environment to inspect'"
    return 0
  fi

  # THE SECRET IS ARMED BEFORE THE FIRST THING THAT CAN BE REDACTED, not after. The
  # `show-environment failed` path below renders tmux's stderr through `claude_auth_redact`,
  # and that boundary is INERT while CLAUDE_AUTH_SECRET is empty — which it was, because the
  # persisted value was not read until further down. A redaction applied before its pattern
  # exists is a redaction in name only. Reading here also removes the duplicate read that
  # sat further down; `__tok`/`__state` are consumed unchanged below.
  claude_auth_read_key_into __tok __state "$__file" "$CLAUDE_AUTH_TOKEN_KEY"
  CLAUDE_AUTH_SECRET="$__tok"

  # ONE INVOCATION, BOTH STREAMS. Running `show-environment -g` twice meant `__err` came
  # from a DIFFERENT invocation than `__rc`/`__out`, so a server that started or died between
  # them produced a failure message with an empty cause (or a cause for a call that
  # succeeded). NOT A PIPE: `$?` is read on its own line.
  local __errf=''
  if __errf=$(mktemp "${TMPDIR:-/tmp}/cqlite-tmuxenv.XXXXXX" 2>/dev/null) && [ -f "$__errf" ]; then
    __out=$(tmux show-environment -g 2>"$__errf")
    __rc=$?
    __err=$(cat "$__errf" 2>/dev/null)
    rm -f "$__errf"
  else
    # No temp file: keep the ONE invocation and say the cause was not captured, rather than
    # taking a second reading of a different moment.
    __out=$(tmux show-environment -g 2>/dev/null)
    __rc=$?
    __err='(stderr not captured: no temporary file could be created)'
  fi
  if [ "$__rc" -ne 0 ]; then
    # TWO WORDINGS MEAN "NO SERVER", and only one of them was recognised. MEASURED on tmux
    # 3.4: a box that has never started a server has no socket, and tmux says `error
    # connecting to <path> (No such file or directory)` — which is precisely the FRESHLY
    # PROVISIONED box this check's primary caller runs on, and it was being reported as
    # "failed for a reason that is not a missing server". A STALE socket (server died,
    # file remains) does say `no server running` (also measured). Anything else — a
    # permission denial, `lost server`, a protocol mismatch — is genuinely UNKNOWN and
    # keeps the UNMEASURED branch below: this list is affirmative, not a catch-all.
    if claude_auth_matches_ci "$__err" 'no server running|error connecting to .*\(no such file or directory\)'; then
      # NOT A DEAD END. A freshly provisioned box has no server yet — that is the NORMAL
      # state at the moment `.agent-ami/profile.yaml` runs bootstrap with --strict — so a
      # blanket NO-SERVER red on this check's primary use case with no way out. The
      # answerable question here is not "what does the live server hold" but "would a NEWLY
      # created server deliver the credential to a pane", and that is directly measurable.
      claude_tmux_cold_verdict_into "$__ov" "$__od" "$__file"
    else
      eval "$__od=\"'tmux show-environment -g' failed for a reason that is not a missing server: \$(claude_auth_redact \"\$__err\")\""
    fi
    return 0
  fi

  claude_tmux_show_key_into __stok  __sstate    "$__out" "$CLAUDE_AUTH_TOKEN_KEY"
  claude_tmux_show_key_into __scfg  __scfgstate "$__out" "$CLAUDE_AUTH_CONFIG_KEY"

  if [ "$__sstate" != present ] || [ -z "$__stok" ]; then
    eval "$__ov=SERVER-MISSING"
    eval "$__od=\"a tmux server IS running and its global environment carries NO \$CLAUDE_AUTH_TOKEN_KEY (\$__sstate) — every pane it spawns lands on the first-run login chooser, whatever \$__file says\""
    return 0
  fi

  # `__tok`/`__state` were read at the top of this function (see the note there); re-reading
  # would be a second reading of a file that may have changed underneath us.
  if [ "$__state" != present ] || [ -z "$__tok" ]; then
    eval "$__od=\"the server carries a \$CLAUDE_AUTH_TOKEN_KEY (SET) but \$__file provides no persisted value to compare it against (\$__state), so whether the server is CURRENT is UNKNOWN\""
    return 0
  fi
  if [ "$__stok" != "$__tok" ]; then
    eval "$__ov=SERVER-STALE"
    eval "$__od=\"the running server's \$CLAUDE_AUTH_TOKEN_KEY DIFFERS from the one persisted in \$__file — the server predates provisioning (or was seeded from an older value), so panes get a credential nobody re-checked\""
    return 0
  fi
  if [ "$__scfgstate" != present ] || [ -z "$__scfg" ]; then
    eval "$__ov=SERVER-INCOMPLETE"
    eval "$__od=\"the server's \$CLAUDE_AUTH_TOKEN_KEY MATCHes \$__file but it carries no \$CLAUDE_AUTH_CONFIG_KEY (\$__scfgstate) — 'tmux new-session <command>' runs no login shell, so /etc/profile.d never supplies it either and the pane gets the un-onboarded first-run picker\""
    return 0
  fi

  # ---- the CONFIG DIR half: NONEMPTY IS NOT CORRECT ---------------------------------
  # This used to be the whole test — "the server names something" and otherwise VERIFIED —
  # which is the two-valued predicate CLAUDE.md warns about: only the bad state was tested,
  # so a STALE, WRONG or NONEXISTENT directory inherited the permissive branch. It matters
  # here more than anywhere: a wrong CLAUDE_CONFIG_DIR sends `claude` to an un-onboarded
  # directory, and THAT is the first-run picker this issue exists for. So VERIFIED requires
  # an AFFIRMATIVE match — equal to the persisted value AND the directory exists.
  claude_auth_read_key_into __cfg __cfgstate "$__file" "$CLAUDE_AUTH_CONFIG_KEY"
  if [ "$__cfgstate" != present ] || [ -z "$__cfg" ]; then
    eval "$__od=\"the server names a \$CLAUDE_AUTH_CONFIG_KEY but \$__file provides no persisted value to compare it against (\$__cfgstate) — PRESENT is not CORRECT, and a comparison with nothing is not a verdict\""
    return 0
  fi
  if [ "$__scfg" != "$__cfg" ]; then
    eval "$__ov=SERVER-CONFIG-STALE"
    eval "$__od=\"the server's \$CLAUDE_AUTH_CONFIG_KEY DIFFERS from the one persisted in \$__file (server: \$__scfg) — panes are pointed at a config directory nobody provisioned, which is the un-onboarded first-run picker even though the token is right\""
    return 0
  fi
  # `[ -d ]` is two-valued, so an UNREADABLE parent collapses onto 'does not exist'. That
  # is deliberately the NON-permissive answer: an unknown must never resolve to the good
  # case. Its remedy differs from SERVER-CONFIG-STALE's — re-seeding the server writes the
  # same nonexistent path back, so `--fix-claude-auth` cannot help here.
  if [ ! -d "$__scfg" ]; then
    eval "$__ov=SERVER-CONFIG-NODIR"
    eval "$__od=\"the server's \$CLAUDE_AUTH_CONFIG_KEY MATCHes \$__file but that directory does not exist (or cannot be read as one): \$__scfg — a pane gets a config dir claude will treat as un-onboarded\""
    return 0
  fi
  eval "$__ov=VERIFIED"
  eval "$__od=\"the running tmux server's global environment carries a \$CLAUDE_AUTH_TOKEN_KEY MATCHing \$__file, and a \$CLAUDE_AUTH_CONFIG_KEY MATCHing it whose directory EXISTS — a pane spawned now inherits both\""
}

# ---- the COLD-START probe: what would a NEWLY created server deliver? --------------
# A private, throwaway tmux server started from the PERSISTED environment, one pane, and a
# report of what that pane received. Four constraints, each load-bearing:
#
#  * IT NEVER TOUCHES THE HOST'S LIVE SERVER. Every call carries `-S <socket>` pointing at a
#    socket inside this run's own private working directory, and the server is killed in a
#    trap on EVERY exit path including signals. A stray tmux server left on a fleet box is a
#    real cost, and so — more quietly — is a stray socket file in a directory we do not own.
#  * IT IS STARTED FROM THE PERSISTED SOURCE, NOT FROM OURS. The two credential variables
#    are scrubbed and re-supplied from the pam_env file (BASH_ENV/ENV with them, since a
#    non-interactive shell would re-inject what was just scrubbed). Bootstrap runs inside an
#    already-authenticated session, so a server started from its ambient environment would
#    report success about a value that is not persisted — #3414's lesson.
#  * IT IS BOUNDED, and refuses rather than running unbounded.
#  * THE PANE IS SPAWNED THE WAY A LANE SPAWNER SPAWNS ONE. `tmux new-session <command>`
#    runs the command through `sh -c`, NOT a login shell, so /etc/profile.d never executes
#    for it (fact 5). A probe using a login shell would measure the wrong thing.
#
# RESIDUAL, declared rather than worked around and IDENTICAL to the one the `--auth` probe
# and `tmux setenv -g` already carry: the token is passed to the child in ARGV (`env KEY=…`),
# so it is briefly visible in `ps` to anyone on the box. That is not a new exposure — the
# same value sits in a mode-644 /etc/environment every user can read.
#
# SCOPE, stated rather than implied: what is reconstructed is the CREDENTIAL environment,
# not a whole PAM session. PATH/HOME/TMUX_TMPDIR and the rest are this process's, because
# they are what makes the probe RUNNABLE and they are not the subject. The claim is
# therefore "a server started with the persisted credential environment delivers it to a
# pane", which is exactly the cold-start question.
CLAUDE_AUTH_PROBE_SOCKET=''
CLAUDE_AUTH_PROBE_DIR=''
CLAUDE_AUTH_PROBE_PREV_TRAPS=''

# THE PROBE LIFECYCLE HELPERS ARE SHARED BY BOTH PROBES — the `--auth` one (which registers
# only a throwaway CLAUDE_CONFIG_DIR) and the cold-start tmux one (a directory AND a server
# socket). They were named `..._cold_probe_...` when only the second existed; a name that
# says "cold" while the auth path also depends on it is a comment that lies in the symbol
# table. Cleanup is keyed on what is REGISTERED, so the socket half is simply skipped when
# no socket was armed.
claude_auth_probe_cleanup() {
  if [ -n "$CLAUDE_AUTH_PROBE_SOCKET" ]; then
    # rc is deliberately ignored: tmux `exit-empty` means the server may already be gone,
    # which is a SUCCESSFUL cleanup, not a failure.
    tmux -S "$CLAUDE_AUTH_PROBE_SOCKET" kill-server >/dev/null 2>&1
    CLAUDE_AUTH_PROBE_SOCKET=''
  fi
  if [ -n "$CLAUDE_AUTH_PROBE_DIR" ]; then
    rm -rf "$CLAUDE_AUTH_PROBE_DIR"
    CLAUDE_AUTH_PROBE_DIR=''
  fi
}
claude_auth_probe_restore_traps() {
  trap - EXIT INT TERM HUP
  if [ -n "$CLAUDE_AUTH_PROBE_PREV_TRAPS" ]; then eval "$CLAUDE_AUTH_PROBE_PREV_TRAPS"; fi
  CLAUDE_AUTH_PROBE_PREV_TRAPS=''
}
# On a signal: clean up, put the CALLER'S traps back, then re-raise so the caller's own
# disposition decides what happens. This file is SOURCED by bootstrap, so it must not
# silently replace a caller's trap or force an exit code of its own.
claude_auth_probe_signal() {
  claude_auth_probe_cleanup
  claude_auth_probe_restore_traps
  kill -s "$1" $$
}
claude_auth_probe_arm_traps() {
  CLAUDE_AUTH_PROBE_PREV_TRAPS=$(trap -p EXIT INT TERM HUP)
  trap 'claude_auth_probe_cleanup' EXIT
  trap 'claude_auth_probe_signal INT' INT
  trap 'claude_auth_probe_signal TERM' TERM
  trap 'claude_auth_probe_signal HUP' HUP
}

# claude_tmux_cold_probe_into <ov_ok> <ov_tok> <ov_toklen> <ov_cfg> <ov_why> <tok> <cfg>
# ov_ok is 1 only when a pane actually reported. An EMPTY <tok>/<cfg> means "a cold session
# would receive nothing here", so the variable is left UNSET for the probe — which is what
# pam_env does with an absent assignment.
claude_tmux_cold_probe_into() {
  local __ok="$1" __otok="$2" __olen="$3" __ocfg="$4" __owhy="$5" __ptok="$6" __pcfg="$7"
  local __dir='' __res='' __sock='' __rc=0
  eval "$__ok=0"; eval "$__otok=unset"; eval "$__olen=0"; eval "$__ocfg="; eval "$__owhy="

  if ! claude_auth_resolve_timeout; then
    eval "$__owhy='no timeout/gtimeout on PATH — refusing to start an UNBOUNDED tmux probe'"
    return 0
  fi
  if ! __dir=$(mktemp -d "${TMPDIR:-/tmp}/cqlite-tmux-probe.XXXXXX") || [ ! -d "$__dir" ]; then
    eval "$__owhy='could not create a private working directory for the isolated probe'"
    return 0
  fi
  __res="$__dir/result"; : >"$__res"
  # `-S <path>` INSIDE the private working directory, not `-L <name>`: a `-L` socket lives
  # in the shared /tmp/tmux-<uid>/ and tmux LEAVES THE SOCKET FILE BEHIND when a server
  # self-exits (measured), so every run would litter a directory it does not own. Here the
  # socket is removed with the directory. A unix socket path is bounded (sun_path, 108
  # bytes), so an over-long TMPDIR is a NAMED refusal rather than a mysterious tmux error.
  __sock="$__dir/cqlite-authprobe.sock"
  if [ "${#__sock}" -gt 100 ]; then
    rm -rf "$__dir"
    eval "$__owhy='the private probe socket path would exceed the unix-socket length limit (TMPDIR is too long) — refusing rather than guessing'"
    return 0
  fi
  # The pane command is ONE tmux argument and the paths are interpolated into it, so a
  # working directory carrying a quote or whitespace would change the command's WORD
  # BOUNDARIES rather than merely fail. Refuse by name; do not escape harder.
  case "$__dir" in
    *[\'\"\ ]*|*'	'*)
      rm -rf "$__dir"
      eval "$__owhy='the private working directory path contains a quote or whitespace, which cannot be passed safely as a tmux pane command — refusing'"
      return 0 ;;
  esac
  CLAUDE_AUTH_PROBE_DIR="$__dir"; CLAUDE_AUTH_PROBE_SOCKET="$__sock"
  claude_auth_probe_arm_traps

  # The pane script reports DELIVERY, never the value: set/unset, a LENGTH, and the config
  # directory (a path, not a secret). Nothing it writes carries the credential.
  cat >"$__dir/probe.sh" <<'CLAUDE_AUTH_PROBE'
#!/bin/sh
t="${CLAUDE_CODE_OAUTH_TOKEN-}"
{
  printf 'tok=%s\n' "${CLAUDE_CODE_OAUTH_TOKEN+set}"
  printf 'toklen=%s\n' "${#t}"
  printf 'cfg=%s\n' "${CLAUDE_CONFIG_DIR-}"
  printf 'end\n'
} >"$1"
CLAUDE_AUTH_PROBE

  local -a __e=(env -u BASH_ENV -u ENV -u TMUX -u TMUX_PANE
                -u "$CLAUDE_AUTH_TOKEN_KEY" -u "$CLAUDE_AUTH_CONFIG_KEY")
  [ -z "$__ptok" ] || __e+=("$CLAUDE_AUTH_TOKEN_KEY=$__ptok")
  [ -z "$__pcfg" ] || __e+=("$CLAUDE_AUTH_CONFIG_KEY=$__pcfg")
  # NOT A PIPE: the command runs on its own line and $? is read on the next.
  if [ "$CLAUDE_AUTH_TIMEOUT_KILL" = 1 ]; then
    "${__e[@]}" "$CLAUDE_AUTH_TIMEOUT_BIN" --kill-after=5 "$CLAUDE_AUTH_TMUX_PROBE_BOUND" \
      tmux -S "$__sock" new-session -d -s cqlite-authprobe "sh '$__dir/probe.sh' '$__res'" >/dev/null 2>&1
  else
    "${__e[@]}" "$CLAUDE_AUTH_TIMEOUT_BIN" "$CLAUDE_AUTH_TMUX_PROBE_BOUND" \
      tmux -S "$__sock" new-session -d -s cqlite-authprobe "sh '$__dir/probe.sh' '$__res'" >/dev/null 2>&1
  fi
  __rc=$?
  if [ "$__rc" -ne 0 ]; then
    claude_auth_probe_cleanup; claude_auth_probe_restore_traps
    eval "$__owhy=\"an isolated throwaway tmux server could not be started on a private socket (rc=\$__rc)\""
    return 0
  fi

  # `new-session -d` returns as soon as the session exists, so the pane may not have
  # written yet. The wait is bounded by the SAME timeout binary rather than by a counted
  # `sleep` loop, so a host whose sleep has no sub-second form cannot stretch it.
  "$CLAUDE_AUTH_TIMEOUT_BIN" "$CLAUDE_AUTH_TMUX_PROBE_BOUND" \
    sh -c 'while :; do grep -q "^end$" "$1" 2>/dev/null && exit 0; sleep 0.2; done' _ "$__res" >/dev/null 2>&1
  __rc=$?
  if [ "$__rc" -ne 0 ]; then
    claude_auth_probe_cleanup; claude_auth_probe_restore_traps
    eval "$__owhy=\"the isolated pane did not report within \${CLAUDE_AUTH_TMUX_PROBE_BOUND}s, so what a new server would deliver is UNKNOWN\""
    return 0
  fi

  local __rtok='' __rlen='' __rcfg=''
  __rtok=$(sed -n 's/^tok=//p' "$__res" 2>/dev/null | tail -1)
  __rlen=$(sed -n 's/^toklen=//p' "$__res" 2>/dev/null | tail -1)
  __rcfg=$(sed -n 's/^cfg=//p' "$__res" 2>/dev/null | tail -1)
  claude_auth_probe_cleanup; claude_auth_probe_restore_traps
  case "$__rlen" in ''|*[!0-9]*) __rlen=0 ;; esac
  eval "$__otok=\${__rtok:-unset}"; eval "$__olen=\$__rlen"; eval "$__ocfg=\$__rcfg"
  eval "$__ok=1"
}

# claude_tmux_cold_verdict_into <outvar_verdict> <outvar_detail> <env-file>
# Verdicts: VERIFIED | COLD-START-MISSING | COLD-START-INCOMPLETE | COLD-START-NODIR |
#           NO-SERVER (UNMEASURED-class: the isolated probe could not run).
# The COLD-START-* names are deliberately distinct from the SERVER-* ones: "a live server
# that lacks the credential" and "a would-be server that would lack it" are different
# operator actions, and a pasted log has to keep them apart.
claude_tmux_cold_verdict_into() {
  local __ov="$1" __od="$2" __file="$3"
  local __tok='' __state='' __cfg='' __cfgstate=''
  # The out-var names here must NOT collide with the callee's own local parameter names:
  # `eval "$__ok=1"` on a shadowed `__ok` evaluates `0=1`. Hence the `__pr*` prefix.
  local __prok=0 __prtok='' __prlen=0 __prcfg='' __prwhy=''
  eval "$__ov=UNMEASURED"; eval "$__od="

  claude_auth_read_key_into __tok __state "$__file" "$CLAUDE_AUTH_TOKEN_KEY"
  CLAUDE_AUTH_SECRET="$__tok"
  case "$__state" in
    unreadable|unparseable)
      eval "$__ov=NO-SERVER"
      eval "$__od=\"no tmux server is running, and the cold-start probe cannot be constructed either: \$__file could not be read as a source (\$__state) — UNMEASURED-class, never an ok\""
      return 0 ;;
  esac
  claude_auth_read_key_into __cfg __cfgstate "$__file" "$CLAUDE_AUTH_CONFIG_KEY"
  case "$__cfgstate" in
    unreadable|unparseable)
      eval "$__ov=NO-SERVER"
      eval "$__od=\"no tmux server is running, and \$__file could not be read for \$CLAUDE_AUTH_CONFIG_KEY (\$__cfgstate), so the cold-start probe cannot be constructed — UNMEASURED-class\""
      return 0 ;;
  esac

  claude_tmux_cold_probe_into __prok __prtok __prlen __prcfg __prwhy "$__tok" "$__cfg"
  if [ "$__prok" != 1 ]; then
    eval "$__ov=NO-SERVER"
    eval "$__od=\"no tmux server is running, and the isolated cold-start probe could not run: \$(claude_auth_redact \"\$__prwhy\") — UNMEASURED-class, never an ok\""
    return 0
  fi

  if [ "$__prtok" != set ] || [ "$__prlen" -eq 0 ]; then
    eval "$__ov=COLD-START-MISSING"
    eval "$__od=\"no tmux server is running, and a throwaway one started from \$__file handed its pane NO \$CLAUDE_AUTH_TOKEN_KEY — so the NEXT real server will not either, and every lane it spawns lands on the first-run login chooser\""
    return 0
  fi
  # A delivered value of unexpected length is not a pass and not a failure of the box: the
  # measurement itself is untrustworthy, so it degrades to the UNMEASURED-class verdict.
  if [ "$__prlen" -ne "${#__tok}" ]; then
    eval "$__ov=NO-SERVER"
    eval "$__od='the isolated pane received a CLAUDE_CODE_OAUTH_TOKEN of unexpected length, so the probe measured something other than the persisted value — UNMEASURED-class'"
    return 0
  fi
  if [ -z "$__prcfg" ]; then
    eval "$__ov=COLD-START-INCOMPLETE"
    eval "$__od=\"a throwaway server started from \$__file delivers the \$CLAUDE_AUTH_TOKEN_KEY but NO \$CLAUDE_AUTH_CONFIG_KEY — 'tmux new-session <command>' runs no login shell, so /etc/profile.d never supplies it and the pane gets the un-onboarded first-run picker\""
    return 0
  fi
  if [ "$__prcfg" != "$__cfg" ]; then
    eval "$__ov=NO-SERVER"
    eval "$__od=\"the isolated pane received a \$CLAUDE_AUTH_CONFIG_KEY the probe did not set (\$__prcfg), so the measurement is not about the persisted source — UNMEASURED-class\""
    return 0
  fi
  # Same two-valued caveat as the live path: an unreadable parent collapses onto "does not
  # exist", which is deliberately the NON-permissive answer.
  if [ ! -d "$__prcfg" ]; then
    eval "$__ov=COLD-START-NODIR"
    eval "$__od=\"a throwaway server started from \$__file delivers both variables, but the \$CLAUDE_AUTH_CONFIG_KEY it delivers does not exist as a directory: \$__prcfg — claude will treat it as un-onboarded\""
    return 0
  fi
  eval "$__ov=VERIFIED"
  eval "$__od=\"no live tmux server to inspect, so this was measured COLD: an ISOLATED throwaway server on a private socket, started from \$__file with the inherited credential scrubbed, delivered BOTH \$CLAUDE_AUTH_TOKEN_KEY and an existing \$CLAUDE_AUTH_CONFIG_KEY to a pane — a server created now will too\""
}

# claude_tmux_show_key_into <outvar_value> <outvar_state> <show-environment-output> <key>:
# `tmux show-environment -g` prints `KEY=value` for a set variable and `-KEY` for one
# explicitly removed. Both "not listed" and "listed as removed" are `absent` — a pane
# receives nothing either way.
claude_tmux_show_key_into() {
  local __ov="$1" __os="$2" __rest="$3" __k="$4"
  local __line='' __nl='' __hit='' __found=0 __removed=0
  __nl=$'\n'
  eval "$__ov="; eval "$__os=absent"
  # A BUILTIN LINE WALK, not `printf | grep`: `grep -x`/`grep -m1` EXIT ON THE FIRST MATCH
  # and the producer then takes SIGPIPE, so under `pipefail` a PRESENT key read as ABSENT
  # once the server environment passed one pipe buffer (see the matcher block above). Not a
  # `while read` either: a piped read loop runs in a SUBSHELL and its writes are discarded.
  # `__found` is tracked SEPARATELY from `__hit` because `KEY=` is a legitimate EMPTY
  # assignment, and collapsing it onto "not found" would be the same two-valued read one
  # level down; the callers reject present-but-empty themselves, with their own wording.
  while [ -n "$__rest" ]; do
    case "$__rest" in
      *"$__nl"*) __line=${__rest%%"$__nl"*}; __rest=${__rest#*"$__nl"} ;;
      *)         __line="$__rest"; __rest='' ;;
    esac
    if [ "$__line" = "-$__k" ]; then __removed=1; fi
    case "$__line" in
      "$__k"=*) if [ "$__found" = 0 ]; then __hit=${__line#"$__k"=}; __found=1; fi ;;
    esac
  done
  # The whole text is scanned either way, so an explicit removal wins wherever it appears —
  # exactly as the `grep -qx` precedence did.
  if [ "$__removed" = 1 ]; then return 0; fi
  if [ "$__found" = 1 ]; then eval "$__ov=\$__hit"; eval "$__os=present"; fi
  return 0
}

# ---- the repair: seed the RUNNING server, persist NOTHING new ----------------------
# claude_auth_fix_tmux_env: push the PERSISTED values into the running tmux server so
# panes spawned from now on inherit them. It writes NO file: /etc/environment already holds
# the token, and a second copy on disk is refused on the PRECEDENT of
# openspec/specs/worker-environment-preflight/spec.md — whose "SHALL NOT write the token
# itself to disk" clause sits under the GIT-CREDENTIAL requirement and is about `$GH_TOKEN`,
# so it does not already name this credential. Cited as the precedent it is; the behaviour
# here is deliberately the stronger reading.
#
# RESIDUAL, stated rather than implied: `tmux setenv -g KEY value` passes the value in
# ARGV, so it is briefly visible in `ps` to anyone on the box. The SAME exposure class,
# with a shorter window, applies to BOTH probes' `env KEY=<token> …` invocations — named
# here too, because a residual declared for one call site and silently carried by two reads
# as a bounded exposure when it is not. That is not a new exposure — the same value sits in
# a mode-644 /etc/environment every user can read — and neither tmux nor `env` offers a
# stdin form, so it is declared rather than worked around.
#
# CLAUDE_CONFIG_DIR is seeded from /etc/environment when it is there, else from THIS
# process's environment (the fleet keeps it in /etc/profile.d, which a tmux-spawned lane
# never reads). The source is NAMED in the output, because seeding from an inherited value
# is a different claim from seeding from a persisted one and the operator must be able to
# tell which happened.
claude_auth_fix_tmux_env() {
  local __file='' __tok='' __state='' __cfg='' __cfgstate='' __cfgsrc='' rc=0
  if ! claude_auth_env_file_into __file; then
    printf 'claude-auth: fix REFUSED (the TEST-ONLY seam is set without CQLITE_BOOTSTRAP_TEST_MODE=1)\n'
    return 1
  fi
  if ! command -v tmux >/dev/null 2>&1; then
    printf 'claude-auth: fix SKIPPED (no `tmux` on PATH — nothing to seed)\n'
    return 1
  fi
  claude_auth_read_key_into __tok __state "$__file" "$CLAUDE_AUTH_TOKEN_KEY"
  CLAUDE_AUTH_SECRET="$__tok"
  if [ "$__state" != present ] || [ -z "$__tok" ]; then
    printf 'claude-auth: fix SKIPPED (%s holds no usable %s: %s) — there is nothing to seed FROM; provision it first\n' \
      "$__file" "$CLAUDE_AUTH_TOKEN_KEY" "$__state"
    return 1
  fi
  if ! tmux setenv -g "$CLAUDE_AUTH_TOKEN_KEY" "$__tok" 2>/dev/null; then
    printf 'claude-auth: fix FAILED (tmux would not accept `setenv -g %s` — is a server running?)\n' "$CLAUDE_AUTH_TOKEN_KEY"
    return 1
  fi
  printf 'claude-auth: seeded %s into the running tmux server (value NOT printed; source %s)\n' \
    "$CLAUDE_AUTH_TOKEN_KEY" "$__file"
  claude_auth_read_key_into __cfg __cfgstate "$__file" "$CLAUDE_AUTH_CONFIG_KEY"
  if [ "$__cfgstate" = present ] && [ -n "$__cfg" ]; then
    __cfgsrc="$__file"
  elif [ -n "${CLAUDE_CONFIG_DIR:-}" ]; then
    __cfg="$CLAUDE_CONFIG_DIR"; __cfgsrc='this process environment (the fleet keeps it in /etc/profile.d, which a tmux-spawned lane never reads)'
  else
    printf 'claude-auth: %s could NOT be seeded — it is in neither %s nor this environment; seed it by hand: tmux setenv -g %s <dir>\n' \
      "$CLAUDE_AUTH_CONFIG_KEY" "$__file" "$CLAUDE_AUTH_CONFIG_KEY"
    return 0
  fi
  if tmux setenv -g "$CLAUDE_AUTH_CONFIG_KEY" "$__cfg" 2>/dev/null; then
    printf 'claude-auth: seeded %s=%s into the running tmux server (source: %s)\n' \
      "$CLAUDE_AUTH_CONFIG_KEY" "$__cfg" "$__cfgsrc"
  else
    printf 'claude-auth: fix FAILED (tmux would not accept `setenv -g %s`)\n' "$CLAUDE_AUTH_CONFIG_KEY"
    rc=1
  fi
  return $rc
}

# ---- CLI ---------------------------------------------------------------------------
claude_auth_usage() {
  printf 'usage: %s --auth [--show-probe-output] | --tmux-env | --report | --fix-tmux-env\n' "${0##*/}"
  printf '  --auth          is the PERSISTED credential valid for a cold, non-interactive start?\n'
  printf '                  prints one `claude-auth:` line: VERIFIED|NOT-PERSISTED|FAILED|UNMEASURED\n'
  printf '                  (exit 0 only for VERIFIED). Makes ONE real, bounded `claude -p` call.\n'
  printf '  --tmux-env      does that credential REACH a tmux-spawned pane? prints one\n'
  printf '                  `claude-tmux-env:` line: VERIFIED|SERVER-STALE|SERVER-MISSING|\n'
  printf '                  SERVER-INCOMPLETE|SERVER-CONFIG-STALE|SERVER-CONFIG-NODIR when a\n'
  printf '                  server is running; COLD-START-MISSING|COLD-START-INCOMPLETE|\n'
  printf '                  COLD-START-NODIR when none is (an ISOLATED throwaway server on a\n'
  printf '                  private socket, started from the PERSISTED environment, measures\n'
  printf '                  what a new one would deliver); NO-SERVER|UNMEASURED when nothing\n'
  printf '                  could be measured (exit 0 only for VERIFIED). VERIFIED needs the\n'
  printf '                  CLAUDE_CONFIG_DIR to EQUAL the persisted one and to EXIST.\n'
  printf '  --report        both lines.\n'
  printf '  --fix-tmux-env  seed the RUNNING tmux server from the persisted value, then\n'
  printf '                  re-measure. Writes NO file; the token value is never printed.\n'
}

claude_auth_emit_auth() {
  local v='' d='' p='' show="${1:-0}"
  if [ "$show" = 1 ]; then
    claude_auth_verdict_into v d p
  else
    claude_auth_verdict_into v d
  fi
  printf 'claude-auth: %s (%s)\n' "$v" "$(claude_auth_redact "$d")"
  if [ "$show" = 1 ] && [ -n "$p" ]; then
    printf 'claude-auth probe-output: %s\n' "$(claude_auth_redact "$p")"
  fi
  [ "$v" = VERIFIED ]
}

claude_auth_emit_tmux() {
  local v='' d=''
  claude_tmux_env_verdict_into v d
  printf 'claude-tmux-env: %s (%s)\n' "$v" "$(claude_auth_redact "$d")"
  [ "$v" = VERIFIED ]
}

claude_auth_main() {
  local show=0 rc=0
  case "${1:-}" in
    --auth)
      [ "${2:-}" = --show-probe-output ] && show=1
      claude_auth_emit_auth "$show" || rc=1
      return $rc ;;
    --tmux-env)     claude_auth_emit_tmux || rc=1; return $rc ;;
    --report)
      claude_auth_emit_auth 0 || rc=1
      claude_auth_emit_tmux  || rc=1
      return $rc ;;
    --fix-tmux-env) claude_auth_fix_tmux_env || rc=1; claude_auth_emit_tmux || rc=1; return $rc ;;
    -h|--help|'')   claude_auth_usage ;;
    *) printf 'claude-auth-capability: unknown arg: %s\n' "$1" >&2; claude_auth_usage >&2; return 2 ;;
  esac
}

# Executed directly (never when sourced): shell options are set HERE, inside the guard, so
# sourcing can never change a caller's `set` flags.
if [ "${BASH_SOURCE[0]}" = "${0}" ]; then
  set -uo pipefail
  claude_auth_main "$@"
fi
