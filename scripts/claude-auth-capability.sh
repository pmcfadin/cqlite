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
#                     VERIFIED | SERVER-STALE | SERVER-MISSING | SERVER-INCOMPLETE |
#                     SERVER-CONFIG-STALE | SERVER-CONFIG-NODIR | NO-SERVER | UNMEASURED
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
# ONLY `VERIFIED` IS A SUCCESS, on either line. NO-SERVER is UNMEASURED-class: nothing was
# running to ask, so nothing was measured. An unmeasured capability never inherits the
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
# THE TOKEN VALUE IS NEVER PRINTED — not to stdout, not to a log, not into a diagnostic.
# Everything reports SET/ABSENT/MATCH/DIFFERS. Comparison is by string equality on the
# extracted VALUE of both sides (never on a reconstructed `KEY=value` line: extracting the
# two sides differently is what produced a false finding on this issue's own thread).
# Every emitted detail goes through ONE boundary, `claude_auth_redact`, so a probe that
# quotes the credential back at us cannot relay it into a verdict line.
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
CLAUDE_AUTH_SENTINEL='CQLITE_CLAUDE_AUTH_OK'
CLAUDE_AUTH_PROMPT="Reply with exactly this word and nothing else: $CLAUDE_AUTH_SENTINEL"
CLAUDE_AUTH_PROBE_BOUND=90

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
# parse ONE assignment the way pam_env reads the file —
#   * whole-line `#` comments are skipped (the anchor forbids a leading `#`);
#   * NO inline-comment stripping: pam_env takes a trailing `# ...` as part of the value;
#   * the LAST assignment wins if a file somehow carries two.
# States: present | absent | absent-file | unreadable | unparseable.
# A SYMLINK is `unreadable`, not followed: what it points at is not the file whose bytes
# pam_env consumes, and an unknown must never resolve to the good case.
claude_auth_read_key_into() {
  local __ov="$1" __os="$2" __f="$3" __k="$4" __raw
  eval "$__ov="; eval "$__os=unreadable"
  if [ ! -e "$__f" ]; then eval "$__os=absent-file"; return 0; fi
  if [ -L "$__f" ] || [ ! -f "$__f" ] || [ ! -r "$__f" ]; then eval "$__os=unreadable"; return 0; fi
  if ! grep -Eq "^[[:space:]]*$__k[[:space:]]*=" "$__f" 2>/dev/null; then
    eval "$__os=absent"; return 0
  fi
  # A SENTINEL PREFIX, because an EMPTY capture and a FAILED capture are otherwise the
  # same string: `KEY=` is a legitimate (empty) assignment while sed producing nothing
  # means the parse failed. Without the marker the `unparseable` branch is unreachable.
  __raw=$(sed -n "s/^[[:space:]]*$__k[[:space:]]*=/VAL:/p" "$__f" 2>/dev/null | tail -1)
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
    eval "$__od='no `claude` on PATH — the persisted credential exists but nothing here can exercise it'"
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
  rm -rf "$__cfg"
  [ -z "$__op" ] || eval "$__op=\"\$(claude_auth_redact \"\$__out\")\""

  if [ "$__rc" -eq 0 ] && printf '%s' "$__out" | grep -qF -- "$CLAUDE_AUTH_SENTINEL"; then
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
  if printf '%s' "$__out" | grep -qEi 'getaddrinfo|ENOTFOUND|ECONNREFUSED|EAI_AGAIN|network is unreachable|temporary failure in name resolution|connection error'; then
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
    eval "$__od='no `tmux` on PATH — there is no server environment to inspect'"
    return 0
  fi

  __err=$(tmux show-environment -g 2>&1 >/dev/null)
  __out=$(tmux show-environment -g 2>/dev/null)
  __rc=$?
  if [ "$__rc" -ne 0 ]; then
    if printf '%s' "$__err" | grep -qi 'no server running'; then
      eval "$__ov=NO-SERVER"
      eval "$__od='no tmux server is running, so there is no pane environment to measure — this is UNMEASURED-class, never an ok: the next server inherits whatever environment STARTS it'"
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

  claude_auth_read_key_into __tok __state "$__file" "$CLAUDE_AUTH_TOKEN_KEY"
  CLAUDE_AUTH_SECRET="$__tok"
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

# claude_tmux_show_key_into <outvar_value> <outvar_state> <show-environment-output> <key>:
# `tmux show-environment -g` prints `KEY=value` for a set variable and `-KEY` for one
# explicitly removed. Both "not listed" and "listed as removed" are `absent` — a pane
# receives nothing either way.
claude_tmux_show_key_into() {
  local __ov="$1" __os="$2" __text="$3" __k="$4" __line
  eval "$__ov="; eval "$__os=absent"
  if printf '%s\n' "$__text" | grep -qx -- "-$__k"; then return 0; fi
  __line=$(printf '%s\n' "$__text" | grep -m1 "^$__k=") || return 0
  eval "$__ov=\${__line#\$__k=}"
  eval "$__os=present"
}

# ---- the repair: seed the RUNNING server, persist NOTHING new ----------------------
# claude_auth_fix_tmux_env: push the PERSISTED values into the running tmux server so
# panes spawned from now on inherit them. It writes NO file: /etc/environment already
# holds the token and a second copy is what
# openspec/specs/worker-environment-preflight/spec.md forbids.
#
# RESIDUAL, stated rather than implied: `tmux setenv -g KEY value` passes the value in
# ARGV, so it is briefly visible in `ps` to anyone on the box. That is not a new exposure
# — the same value sits in a mode-644 /etc/environment every user can read — and tmux
# offers no stdin form, so it is declared rather than worked around.
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
  printf '                  SERVER-INCOMPLETE|SERVER-CONFIG-STALE|SERVER-CONFIG-NODIR|\n'
  printf '                  NO-SERVER|UNMEASURED (exit 0 only for VERIFIED). VERIFIED needs the\n'
  printf '                  server CLAUDE_CONFIG_DIR to EQUAL the persisted one and to EXIST.\n'
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
