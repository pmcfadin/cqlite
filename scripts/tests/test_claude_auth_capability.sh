#!/usr/bin/env bash
# Regression test for CLAUDE CREDENTIAL REACHABILITY (issue #3733):
# scripts/claude-auth-capability.sh and the bootstrap section that surfaces it.
#
# THE FIELD FAILURE IT EXISTS FOR. A newly created tmux session on a fleet box lands on
# claude's first-run login chooser, so a retired lane cannot be replaced. The causal
# chain, measured on the box (issue #3733):
#   1. the ONLY working credential is the env var CLAUDE_CODE_OAUTH_TOKEN;
#      $CLAUDE_CONFIG_DIR/.credentials.json holds EMPTY tokens and authenticates nothing;
#   2. the token authenticates INDEPENDENTLY of CLAUDE_CONFIG_DIR;
#   3. the token is provisioned in /etc/environment ONLY, which pam_env reads;
#   4. a tmux pane's environment comes from the tmux SERVER, fixed at server start, so a
#      server that predates provisioning yields panes with neither variable;
#   5. `tmux new-session <command>` does not run a login shell, so /etc/profile.d never
#      executes for a spawned lane either;
#   6. therefore NOTHING ON DISK distinguishes a working box from a broken one — the
#      distinguishing state is a long-running process's start environment.
# Hence two independent verdicts, and hence this suite drives them independently.
#
# WHAT IT ASSERTS BEYOND "THE CODE IS THERE":
#   * every verdict is REACHABLE and correctly labeled, by planting its condition;
#   * an INHERITED CLAUDE_CODE_OAUTH_TOKEN can never produce a positive verdict about a
#     PERSISTED one (#3414's shipped defect, one subject over) — the scrub case;
#   * rc alone and output alone are each INSUFFICIENT for VERIFIED (both halves required);
#   * NO run prints a token-shaped value anywhere on stdout/stderr.
#
# HOST SAFETY. Nothing here reads the real /etc/environment or touches the real tmux
# server: the env-file seam stands in (inert without CQLITE_BOOTSTRAP_TEST_MODE=1) and
# `claude`/`tmux` are recording PATH shims. No network call is ever made.
#
# Run standalone:   bash scripts/tests/test_claude_auth_capability.sh
# Or via the gate:  scripts/agent-gate.sh runs it in the `tooling-tests` component.
set -uo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)          # scripts/tests
REPO_ROOT=$(cd "$SCRIPT_DIR/../.." && pwd)
CAPLIB="$REPO_ROOT/scripts/claude-auth-capability.sh"
BOOTSTRAP="$REPO_ROOT/scripts/bootstrap-agent-machine.sh"

PASS=0
FAIL=0
SKIP=0
ok()   { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad()  { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }
skip() { printf 'SKIP - %s  [reason: %s]\n' "$1" "$2"; SKIP=$((SKIP + 1)); }

# `mktemp` CAN FAIL and this suite deliberately runs WITHOUT `set -e`: an empty $tmp would
# make every path below a ROOT-LEVEL path and the EXIT trap `rm -rf ""`. Same guard, and the
# same reasoning, as scripts/tests/lib/perf-capability-test-lib.sh.
if ! tmp=$(mktemp -d "${TMPDIR:-/tmp}/claude-auth-test.XXXXXX"); then tmp=''; fi
case "$tmp" in /*) ;; *) tmp='' ;; esac
if [ -z "$tmp" ] || [ ! -d "$tmp" ]; then
  printf 'test_claude_auth_capability: REFUSING TO RUN (unusable-temp-dir): mktemp -d did not yield an existing absolute directory (got %s).\n' \
    "'${tmp:-<empty>}'" >&2
  exit 1
fi
trap 'rm -rf "$tmp"' EXIT
if tmp_canon=$(cd -P -- "$tmp" 2>/dev/null && pwd -P) && [ -n "$tmp_canon" ] && [ -d "$tmp_canon" ]; then
  tmp="$tmp_canon"
fi
umask 022

# THE PLANTED SECRET. Every case that needs a token uses this literal, and the final case
# greps the WHOLE captured transcript for it: a suite that leaks the value it is asserting
# is never printed would be the exact defect wearing a green tally.
TOK='sk-cqlite-test-TOKEN-3733-do-not-print'
TOK_OTHER='sk-cqlite-test-STALE-3733-do-not-print'
SENTINEL='CQLITE_CLAUDE_AUTH_OK'

# THE CONFIG-DIR FIXTURES ARE REAL PATHS, because VERIFIED now requires the directory to
# EXIST — a nonexistent CLAUDE_CONFIG_DIR sends `claude` to an un-onboarded config and
# produces the first-run picker, which is this issue's reported symptom. CFGDIR exists,
# CFGDIR_OTHER exists and is a DIFFERENT directory, CFGDIR_GHOST is never created.
CFGDIR="$tmp/claude-config";       mkdir -p "$CFGDIR"
CFGDIR_OTHER="$tmp/claude-config-other"; mkdir -p "$CFGDIR_OTHER"
CFGDIR_GHOST="$tmp/claude-config-never-created"

# EVERY case's combined stdout+stderr is appended here, and case "no leak" greps it once.
TRANSCRIPT="$tmp/transcript.log"; : >"$TRANSCRIPT"
# run_cap <shimdir> <envfile-or-empty> [extra env assignments...] -- <cap args...>
# Runs the capability script with a PATH whose FIRST entry is <shimdir>, records the
# combined output into $TRANSCRIPT, and leaves it in $out / $rc.
out=''; rc=0
run_cap() {
  local shimdir="$1" envfile="$2"; shift 2
  local -a pre=()
  while [ "$#" -gt 0 ] && [ "$1" != '--' ]; do pre+=("$1"); shift; done
  [ "${1:-}" = '--' ] && shift
  rc=0
  out=$(PATH="$shimdir:$PATH" env \
        CQLITE_BOOTSTRAP_TEST_MODE=1 \
        CQLITE_CLAUDE_AUTH_ENV_FILE="$envfile" \
        ${pre[@]+"${pre[@]}"} \
        bash "$CAPLIB" "$@" 2>&1) || rc=$?
  printf '%s\n' "$out" >>"$TRANSCRIPT"
}

# --- shim factory -------------------------------------------------------------------
# mkshim <dir>: a fresh PATH directory holding NOTHING (so `claude`/`tmux` are absent
# unless a case plants them). `/usr/bin` etc. still follow on PATH, so a case that wants
# a tool ABSENT must plant a refusing stub rather than rely on an empty dir.
mkshim() { local d="$1"; rm -rf "$d"; mkdir -p "$d"; printf '%s' "$d"; }

# plant_claude <dir> <rc> <stdout-text>
plant_claude() {
  local d="$1" prc="$2" text="$3"
  cat >"$d/claude" <<EOF
#!/usr/bin/env bash
# Recording stub. It NEVER echoes its own argv or environment: the value under test is a
# secret, and a stub that prints what it was handed would leak it through the suite.
printf '%s\n' "$text"
exit $prc
EOF
  chmod +x "$d/claude"
}
# plant_claude_echoing_token <dir>: rc 0, and it prints the TOKEN it was handed. Used by
# the redaction case: the capability script must not relay a secret out of probe output.
plant_claude_echoing_token() {
  cat >"$1/claude" <<'EOF'
#!/usr/bin/env bash
printf 'auth error for credential %s\n' "${CLAUDE_CODE_OAUTH_TOKEN-<unset>}"
exit 1
EOF
  chmod +x "$1/claude"
}
# plant_claude_reporting_scrub <dir>: rc 0, prints which value it actually received,
# LENGTH-ONLY plus a marker naming the source, so a case can assert the scrub without the
# suite ever holding the secret in its own output.
plant_claude_probe_env() {
  cat >"$1/claude" <<EOF
#!/usr/bin/env bash
t="\${CLAUDE_CODE_OAUTH_TOKEN-}"
if [ "\$t" = '$TOK' ]; then printf 'saw=persisted\n'
elif [ -n "\$t" ]; then printf 'saw=other-len=%s\n' "\${#t}"
else printf 'saw=unset\n'; fi
printf 'cfgdir=%s\n' "\${CLAUDE_CONFIG_DIR:+set}"
printf '%s\n' '$SENTINEL'
exit 0
EOF
  chmod +x "$1/claude"
}
# plant_tmux <dir> <mode>: a `tmux` stub. Modes mirror the states a fleet box has.
#   no-server   — `show-environment -g` fails the way a serverless box does
#   missing     — server env WITHOUT the token
#   stale       — server env with a DIFFERENT token
#   incomplete  — matching token, NO CLAUDE_CONFIG_DIR
#   complete    — matching token AND CLAUDE_CONFIG_DIR
#   broken      — an error that is NOT "no server running"
# `setenv` is always accepted and RECORDED — as key plus VALUE LENGTH, never the value, so
# the recording file cannot become the leak this suite forbids.
plant_tmux() {
  local d="$1" mode="$2" cfg="${3:-$CFGDIR}"
  cat >"$d/tmux" <<EOF
#!/usr/bin/env bash
log="$d/tmux-calls.log"
case "\$1" in
  show-environment)
    case '$mode' in
      no-server)  printf 'no server running on /tmp/tmux-1000/default\n' >&2; exit 1 ;;
      broken)     printf 'lost server\n' >&2; exit 1 ;;
      missing)    printf 'CLAUDE_CONFIG_DIR=%s\nPATH=/usr/bin\n' '$cfg'; exit 0 ;;
      stale)      printf 'CLAUDE_CODE_OAUTH_TOKEN=%s\nCLAUDE_CONFIG_DIR=%s\n' '$TOK_OTHER' '$cfg'; exit 0 ;;
      incomplete) printf 'CLAUDE_CODE_OAUTH_TOKEN=%s\n-CLAUDE_CONFIG_DIR\n' '$TOK'; exit 0 ;;
      complete)   printf 'CLAUDE_CODE_OAUTH_TOKEN=%s\nCLAUDE_CONFIG_DIR=%s\n' '$TOK' '$cfg'; exit 0 ;;
    esac ;;
  set-environment|setenv)
    shift
    key=''; val=''
    while [ "\$#" -gt 0 ]; do
      case "\$1" in -*) ;; *) if [ -z "\$key" ]; then key="\$1"; else val="\$1"; fi ;; esac
      shift
    done
    printf 'setenv %s len=%s\n' "\$key" "\${#val}" >>"\$log"
    exit 0 ;;
esac
exit 0
EOF
  chmod +x "$d/tmux"
}
# plant_absent <dir> <tool>: make <tool> UNRESOLVABLE even though /usr/bin is on PATH.
# A stub that exits 127 is not the same fact as an absent binary, so this suite instead
# runs the capability script with a MINIMAL PATH holding only the shim dir plus coreutils.
# minpath: a PATH holding ONLY the coreutils the capability script needs — so `claude`
# and `tmux` are genuinely ABSENT rather than stubbed. A stub exiting 127 is not the same
# FACT as a missing binary, and this suite asserts the missing-binary verdict.
MINPATH="$tmp/minpath"; mkdir -p "$MINPATH"
for t in bash sh uname grep sed tail cut tr mktemp rm env cat head sort comm find chmod ln mkdir timeout; do
  src=$(command -v "$t" 2>/dev/null) || continue
  ln -sf "$src" "$MINPATH/$t"
done
if [ ! -x "$MINPATH/timeout" ] || [ ! -x "$MINPATH/grep" ]; then
  printf 'test_claude_auth_capability: REFUSING TO RUN (unusable-minimal-PATH): could not link the coreutils the absent-binary cases need.\n' >&2
  exit 1
fi

mkenvfile() { # mkenvfile <path> <line...>
  local p="$1"; shift
  : >"$p"
  local l
  for l in "$@"; do printf '%s\n' "$l" >>"$p"; done
}

# =====================================================================================
# 0. The script exists and is sourceable/executable at all.
# =====================================================================================
if [ ! -r "$CAPLIB" ]; then
  bad "scripts/claude-auth-capability.sh is missing from this checkout"
  printf '\n== summary ==\npass=%s fail=%s skip=%s\n' "$PASS" "$FAIL" "$SKIP"
  exit 1
fi
ok "scripts/claude-auth-capability.sh present"

# =====================================================================================
# 1. NOT-PERSISTED — no token line in the pam_env file. THE FIELD DEFAULT on an
#    unprovisioned box, and the remedy differs from FAILED's, so it is its own verdict.
# =====================================================================================
d=$(mkshim "$tmp/s1"); ef="$tmp/env1"
mkenvfile "$ef" '# a comment' "CLAUDE_CONFIG_DIR=$CFGDIR" 'PATH=/usr/bin'
plant_claude_probe_env "$d"
plant_tmux "$d" complete
run_cap "$d" "$ef" -- --auth
if printf '%s' "$out" | grep -q '^claude-auth: NOT-PERSISTED'; then
  ok "claude-auth: NOT-PERSISTED when the pam_env file carries no CLAUDE_CODE_OAUTH_TOKEN"
else
  bad "claude-auth: expected NOT-PERSISTED, got: $out"
fi
if [ "$rc" -ne 0 ]; then
  ok "claude-auth: a non-VERIFIED verdict exits non-zero"
else
  bad "claude-auth: NOT-PERSISTED exited 0 — a non-verdict must never read as success"
fi

# =====================================================================================
# 1b. THE SCRUB (issue #3414's shipped defect, one subject over). Bootstrap runs inside a
#     session that ALREADY carries the token, so an unscrubbed check answers about the
#     INHERITED value while claiming to answer about the PERSISTED one. With nothing
#     persisted and a perfectly good token in the environment, the verdict must still be
#     NOT-PERSISTED — never VERIFIED.
# =====================================================================================
run_cap "$d" "$ef" "CLAUDE_CODE_OAUTH_TOKEN=$TOK" "BASH_ENV=$tmp/nonexistent-bashenv" -- --auth
if printf '%s' "$out" | grep -q '^claude-auth: NOT-PERSISTED'; then
  ok "claude-auth: an INHERITED token does NOT satisfy the persisted-credential question (the scrub)"
else
  bad "claude-auth: an inherited token produced '$out' — the scrub is not in effect"
fi

# =====================================================================================
# 2. VERIFIED — persisted token, and the probe returns BOTH rc 0 AND the sentinel.
# =====================================================================================
d2=$(mkshim "$tmp/s2"); ef2="$tmp/env2"
mkenvfile "$ef2" "CLAUDE_CODE_OAUTH_TOKEN=$TOK" "CLAUDE_CONFIG_DIR=$CFGDIR"
plant_claude_probe_env "$d2"
run_cap "$d2" "$ef2" -- --auth
if printf '%s' "$out" | grep -q '^claude-auth: VERIFIED'; then
  ok "claude-auth: VERIFIED when the persisted token authenticates a cold non-interactive run"
else
  bad "claude-auth: expected VERIFIED, got: $out"
fi
if [ "$rc" -eq 0 ]; then
  ok "claude-auth: VERIFIED exits 0"
else
  bad "claude-auth: VERIFIED exited $rc"
fi
# ...and the value the probe actually received came from the FILE, not from the ambient
# environment: the stub reports `saw=persisted` only for the planted literal.
run_cap "$d2" "$ef2" "CLAUDE_CODE_OAUTH_TOKEN=$TOK_OTHER" -- --auth --show-probe-output
if printf '%s' "$out" | grep -q 'saw=persisted'; then
  ok "claude-auth: the probe is handed the PERSISTED value even when a DIFFERENT one is inherited"
else
  bad "claude-auth: the probe did not receive the persisted value: $out"
fi

# =====================================================================================
# 3. FAILED — persisted, but it does not authenticate. Textually distinct from
#    NOT-PERSISTED because the operator action differs (replace vs provision).
# =====================================================================================
d3=$(mkshim "$tmp/s3")
plant_claude "$d3" 1 'Failed to authenticate: OAuth session expired and could not be refreshed'
run_cap "$d3" "$ef2" -- --auth
if printf '%s' "$out" | grep -q '^claude-auth: FAILED'; then
  ok "claude-auth: FAILED when the persisted token is rejected"
else
  bad "claude-auth: expected FAILED, got: $out"
fi

# =====================================================================================
# 4. rc ALONE IS NOT ENOUGH. `claude -p` exiting 0 having printed nothing cannot
#    distinguish "authenticated" from "authenticated and returned nothing", so the
#    sentinel is required too.
# =====================================================================================
d4=$(mkshim "$tmp/s4")
plant_claude "$d4" 0 ''
run_cap "$d4" "$ef2" -- --auth
if printf '%s' "$out" | grep -q '^claude-auth: FAILED'; then
  ok "claude-auth: rc 0 with NO sentinel is not VERIFIED"
else
  bad "claude-auth: rc 0 + empty output produced '$out' — rc alone must not certify"
fi

# =====================================================================================
# 5. OUTPUT ALONE IS NOT ENOUGH either — the sentinel with a non-zero rc is a failure.
# =====================================================================================
d5=$(mkshim "$tmp/s5")
plant_claude "$d5" 1 "$SENTINEL"
run_cap "$d5" "$ef2" -- --auth
if printf '%s' "$out" | grep -q '^claude-auth: FAILED'; then
  ok "claude-auth: the sentinel with rc != 0 is not VERIFIED"
else
  bad "claude-auth: sentinel + rc 1 produced '$out' — output alone must not certify"
fi

# =====================================================================================
# 6. UNMEASURED — no `claude` on PATH. An unmeasured capability must never inherit the
#    permissive branch.
# =====================================================================================
d6=$(mkshim "$tmp/s6")
rc=0
out=$(PATH="$d6:$MINPATH" env CQLITE_BOOTSTRAP_TEST_MODE=1 CQLITE_CLAUDE_AUTH_ENV_FILE="$ef2" \
      bash "$CAPLIB" --auth 2>&1) || rc=$?
printf '%s\n' "$out" >>"$TRANSCRIPT"
if printf '%s' "$out" | grep -q '^claude-auth: UNMEASURED'; then
  ok "claude-auth: UNMEASURED when no 'claude' binary is on PATH"
else
  bad "claude-auth: expected UNMEASURED with no claude, got: $out"
fi

# =====================================================================================
# 7. UNMEASURED — the pam_env file cannot be read as itself. A SYMLINK is refused rather
#    than followed: what it points at is not the file whose contents pam_env consumes.
# =====================================================================================
d7=$(mkshim "$tmp/s7"); plant_claude_probe_env "$d7"
ln -s "$ef2" "$tmp/env-link"
run_cap "$d7" "$tmp/env-link" -- --auth
if printf '%s' "$out" | grep -q '^claude-auth: UNMEASURED'; then
  ok "claude-auth: UNMEASURED when the pam_env file is a symlink (unreadable AS ITSELF)"
else
  bad "claude-auth: expected UNMEASURED for a symlinked env file, got: $out"
fi

# =====================================================================================
# 8-12. THE TMUX DIMENSION — the one that actually failed in the field and that no
#       existing check covers. A pane's environment comes from the SERVER.
# =====================================================================================
for mode_case in \
  "no-server:NO-SERVER:no tmux server is running" \
  "missing:SERVER-MISSING:the running server carries no token (THE field failure)" \
  "stale:SERVER-STALE:the running server carries a DIFFERENT token" \
  "incomplete:SERVER-INCOMPLETE:token matches but CLAUDE_CONFIG_DIR is absent" \
  "complete:VERIFIED:token matches and CLAUDE_CONFIG_DIR is present" \
  "broken:UNMEASURED:tmux failed for a reason that is not a missing server"
do
  mode="${mode_case%%:*}"; rest="${mode_case#*:}"
  want="${rest%%:*}"; why="${rest#*:}"
  dt=$(mkshim "$tmp/st-$mode")
  plant_tmux "$dt" "$mode"
  run_cap "$dt" "$ef2" -- --tmux-env
  if printf '%s' "$out" | grep -q "^claude-tmux-env: $want"; then
    ok "claude-tmux-env: $want — $why"
  else
    bad "claude-tmux-env: expected $want for mode '$mode', got: $out"
  fi
done

# 13. UNMEASURED — no tmux binary at all (distinct from NO-SERVER: nothing to ask).
d13=$(mkshim "$tmp/s13")
rc=0
out=$(PATH="$d13:$MINPATH" env CQLITE_BOOTSTRAP_TEST_MODE=1 CQLITE_CLAUDE_AUTH_ENV_FILE="$ef2" \
      bash "$CAPLIB" --tmux-env 2>&1) || rc=$?
printf '%s\n' "$out" >>"$TRANSCRIPT"
if printf '%s' "$out" | grep -q '^claude-tmux-env: UNMEASURED'; then
  ok "claude-tmux-env: UNMEASURED when no 'tmux' binary exists"
else
  bad "claude-tmux-env: expected UNMEASURED with no tmux, got: $out"
fi

# 14. UNMEASURED — a server value exists but there is NO PERSISTED BASELINE to compare it
#     against. "Present" is not "correct"; a comparison with nothing is not a verdict.
d14=$(mkshim "$tmp/s14"); plant_tmux "$d14" complete
run_cap "$d14" "$ef" -- --tmux-env
if printf '%s' "$out" | grep -q '^claude-tmux-env: UNMEASURED'; then
  ok "claude-tmux-env: UNMEASURED when the server has a token but nothing is persisted to compare it to"
else
  bad "claude-tmux-env: expected UNMEASURED with no persisted baseline, got: $out"
fi

# =====================================================================================
# 15. --fix-tmux-env seeds the RUNNING server from the PERSISTED value, and persists
#     NOTHING new: no second copy of the secret anywhere on disk.
# =====================================================================================
d15=$(mkshim "$tmp/s15"); plant_tmux "$d15" missing
find "$tmp" -type f 2>/dev/null | sort >"$tmp/files-before"
run_cap "$d15" "$ef2" -- --fix-tmux-env
find "$tmp" -type f 2>/dev/null | sort >"$tmp/files-after"
if [ -f "$d15/tmux-calls.log" ] && grep -q '^setenv CLAUDE_CODE_OAUTH_TOKEN len=' "$d15/tmux-calls.log"; then
  ok "--fix-tmux-env: seeds CLAUDE_CODE_OAUTH_TOKEN into the running server"
else
  bad "--fix-tmux-env: no setenv of the token was recorded"
fi
if [ -f "$d15/tmux-calls.log" ] && grep -q '^setenv CLAUDE_CONFIG_DIR len=' "$d15/tmux-calls.log"; then
  ok "--fix-tmux-env: seeds CLAUDE_CONFIG_DIR too (its absence is the theme-picker state)"
else
  bad "--fix-tmux-env: CLAUDE_CONFIG_DIR was not seeded"
fi
# NOTHING new on disk carries the secret. The pam_env file already holds it; a second copy
# is what openspec/specs/worker-environment-preflight/spec.md forbids.
# Only files the FIX ITSELF created are in scope: the suite's own shims and pam_env
# fixture legitimately embed the planted literal. Diffing before/after is what makes this
# a statement about the CODE rather than about the harness.
new_files=$(comm -13 "$tmp/files-before" "$tmp/files-after")
leaked_files=''
while IFS= read -r nf; do
  [ -n "$nf" ] || continue
  case "$nf" in "$TRANSCRIPT"|"$tmp/files-before"|"$tmp/files-after") continue ;; esac
  grep -qF -- "$TOK" "$nf" 2>/dev/null && leaked_files="$leaked_files $nf"
done <<EOF
$new_files
EOF
if [ -z "$leaked_files" ]; then
  ok "--fix-tmux-env: wrote the token to NO new file"
else
  bad "--fix-tmux-env: the token appears in files it should not: $leaked_files"
fi

# =====================================================================================
# 16. REDACTION AT THE EMIT BOUNDARY. A probe whose OUTPUT quotes the credential must not
#     relay it into a verdict line — summary text gets pasted into PR comments and logs.
# =====================================================================================
d16=$(mkshim "$tmp/s16"); plant_claude_echoing_token "$d16"
run_cap "$d16" "$ef2" -- --auth
if printf '%s' "$out" | grep -q '^claude-auth: FAILED' && ! printf '%s' "$out" | grep -qF -- "$TOK"; then
  ok "claude-auth: a probe that echoes the credential is REDACTED at the emit boundary"
else
  bad "claude-auth: the credential survived into the verdict line: $out"
fi

# =====================================================================================
# 17. THE SEAM IS INERT WITHOUT THE TEST MARKER. An env-chosen pam_env path could
#     otherwise fabricate a verdict by accident (a stray export), which is the whole
#     class this file is about.
# =====================================================================================
d17=$(mkshim "$tmp/s17"); plant_claude_probe_env "$d17"
rc=0
out=$(PATH="$d17:$PATH" env -u CQLITE_BOOTSTRAP_TEST_MODE CQLITE_CLAUDE_AUTH_ENV_FILE="$ef2" \
      bash "$CAPLIB" --auth 2>&1) || rc=$?
printf '%s\n' "$out" >>"$TRANSCRIPT"
if printf '%s' "$out" | grep -q '^claude-auth: UNMEASURED' && printf '%s' "$out" | grep -q 'CQLITE_BOOTSTRAP_TEST_MODE'; then
  ok "the env-file seam is inert (and REFUSES loudly) without CQLITE_BOOTSTRAP_TEST_MODE=1"
else
  bad "the env-file seam was honoured without the test marker, or refused silently: $out"
fi

# =====================================================================================
# 18. BOOTSTRAP WIRING — the opt-out is LOUD and NON-PASSING, and the two verdict lines
#     reach the operator through bootstrap's own reporter.
# =====================================================================================
mkroot() {
  local dir="$1"
  mkdir -p "$dir/scripts/lib" "$dir/scripts/flow"
  cp "$BOOTSTRAP" "$dir/scripts/bootstrap-agent-machine.sh"
  cp "$CAPLIB" "$dir/scripts/claude-auth-capability.sh"
  cp "$REPO_ROOT/scripts/perf-capability.sh" "$dir/scripts/perf-capability.sh" 2>/dev/null || true
  cp "$REPO_ROOT/scripts/lib/gate-notify.sh" "$dir/scripts/lib/gate-notify.sh" 2>/dev/null || true
}
bs_root="$tmp/bs-root"; mkroot "$bs_root"
export GIT_CONFIG_GLOBAL="$tmp/global-gitconfig"; export GIT_CONFIG_NOSYSTEM=1; : >"$GIT_CONFIG_GLOBAL"
d18=$(mkshim "$tmp/s18"); plant_claude_probe_env "$d18"; plant_tmux "$d18" complete
bs_out=$(PATH="$d18:$PATH" env CQLITE_BOOTSTRAP_TEST_MODE=1 CQLITE_BOOTSTRAP_SKIP_GATE_PIN=1 \
         CQLITE_CLAUDE_AUTH_ENV_FILE="$ef2" HOME="$tmp/home" \
         bash "$bs_root/scripts/bootstrap-agent-machine.sh" --skip-smoke --skip-push-probe --skip-claude-auth 2>&1)
printf '%s\n' "$bs_out" >>"$TRANSCRIPT"
if printf '%s' "$bs_out" | grep -q 'claude-auth: OPT-OUT'; then
  ok "bootstrap: --skip-claude-auth emits a LOUD claude-auth: OPT-OUT verdict"
else
  bad "bootstrap: --skip-claude-auth produced no OPT-OUT line"
fi
if printf '%s' "$bs_out" | grep -q 'All checks green'; then
  bad "bootstrap: --skip-claude-auth still reported 'All checks green' — an opt-out must never buy one"
else
  ok "bootstrap: the opt-out WITHHOLDS 'All checks green'"
fi
bs_out2=$(PATH="$d18:$PATH" env CQLITE_BOOTSTRAP_TEST_MODE=1 CQLITE_BOOTSTRAP_SKIP_GATE_PIN=1 \
          CQLITE_CLAUDE_AUTH_ENV_FILE="$ef2" HOME="$tmp/home" \
          bash "$bs_root/scripts/bootstrap-agent-machine.sh" --skip-smoke --skip-push-probe 2>&1)
printf '%s\n' "$bs_out2" >>"$TRANSCRIPT"
if printf '%s' "$bs_out2" | grep -q 'claude-auth: VERIFIED' \
   && printf '%s' "$bs_out2" | grep -q 'claude-tmux-env: VERIFIED'; then
  ok "bootstrap: both verdict lines surface through bootstrap's reporter"
else
  bad "bootstrap: the verdict lines are missing from the run"
  printf '%s\n' "$bs_out2" | sed -n '/Claude credential/,/^$/p'
fi
# CONTRADICTORY INTENTS ARE A USAGE ERROR, not a silent resolution (the --fix-gate-pin rule).
usage_rc=0
usage_out=$(bash "$bs_root/scripts/bootstrap-agent-machine.sh" --skip-claude-auth --fix-claude-auth 2>&1) || usage_rc=$?
printf '%s\n' "$usage_out" >>"$TRANSCRIPT"
if [ "$usage_rc" = 2 ] && printf '%s' "$usage_out" | grep -q 'contradictory'; then
  ok "bootstrap: --skip-claude-auth beside --fix-claude-auth is a usage error (exit 2)"
else
  bad "bootstrap: contradictory flags resolved silently (rc=$usage_rc): $usage_out"
fi

# =====================================================================================
# 20a-c. `NONEMPTY` IS NOT `CORRECT` — the CLAUDE_CONFIG_DIR half of the tmux verdict.
#     The check used to be "the server names SOMETHING" else VERIFIED, so a stale, wrong
#     or nonexistent directory read as an [ok]. That is the two-valued-predicate shape
#     CLAUDE.md warns about — only the bad state is tested, so every unknown state inherits
#     the permissive branch — and it undermines the exact verdict this issue turns on: a
#     wrong config dir sends `claude` to an un-onboarded directory, which IS the first-run
#     picker. VERIFIED now requires an AFFIRMATIVE match against the persisted value AND
#     the directory to exist; each failure keeps its own name because the remedies differ
#     (re-seed the server vs. provision the directory vs. nothing to compare against).
# =====================================================================================
d20a=$(mkshim "$tmp/s20a"); plant_tmux "$d20a" complete "$CFGDIR_OTHER"
run_cap "$d20a" "$ef2" -- --tmux-env
if printf '%s' "$out" | grep -q '^claude-tmux-env: SERVER-CONFIG-STALE'; then
  ok "claude-tmux-env: SERVER-CONFIG-STALE when the server's CLAUDE_CONFIG_DIR DIFFERS from the persisted one"
else
  bad "claude-tmux-env: a differing config dir did not get its own verdict: $out"
fi

ef_ghost="$tmp/env-ghost-cfg"
mkenvfile "$ef_ghost" "CLAUDE_CODE_OAUTH_TOKEN=$TOK" "CLAUDE_CONFIG_DIR=$CFGDIR_GHOST"
d20b=$(mkshim "$tmp/s20b"); plant_tmux "$d20b" complete "$CFGDIR_GHOST"
run_cap "$d20b" "$ef_ghost" -- --tmux-env
if printf '%s' "$out" | grep -q '^claude-tmux-env: SERVER-CONFIG-NODIR'; then
  ok "claude-tmux-env: SERVER-CONFIG-NODIR when the config dir MATCHES the persisted value but does not exist"
else
  bad "claude-tmux-env: a nonexistent config dir was accepted: $out"
fi

# No persisted CLAUDE_CONFIG_DIR at all: there is nothing to compare the server against,
# and a comparison with nothing is not a verdict (the same rule case 14 applies to the
# token). UNMEASURED, never VERIFIED.
ef_nocfg="$tmp/env-nocfg"
mkenvfile "$ef_nocfg" "CLAUDE_CODE_OAUTH_TOKEN=$TOK"
d20c=$(mkshim "$tmp/s20c"); plant_tmux "$d20c" complete
run_cap "$d20c" "$ef_nocfg" -- --tmux-env
if printf '%s' "$out" | grep -q '^claude-tmux-env: UNMEASURED'; then
  ok "claude-tmux-env: UNMEASURED when nothing persisted names a CLAUDE_CONFIG_DIR to compare against"
else
  bad "claude-tmux-env: an unverifiable config dir inherited the permissive branch: $out"
fi

# =====================================================================================
# 20. THE PLATFORM GUARD APPLIES TO **BOTH** VERDICTS. /etc/environment + pam_env is a
#     Linux mechanism, and the header block has always documented both lines as UNMEASURED
#     off Linux — but the guard was applied only to `claude-auth:`, so a macOS host could
#     emit `claude-tmux-env: VERIFIED` and recommend Linux-only /etc/environment remedies.
#     A false VERIFIED is an [ok], and [ok] is what `--strict` reads (#3414: scoping a
#     platform out is not the same as passing it).
# =====================================================================================
REAL_UNAME=$(command -v uname 2>/dev/null)
if [ -z "$REAL_UNAME" ]; then
  skip "platform guard: both verdicts UNMEASURED off Linux" "no uname on PATH to stub"
else
  d20=$(mkshim "$tmp/s20"); plant_claude_probe_env "$d20"; plant_tmux "$d20" complete
  cat >"$d20/uname" <<EOF
#!/usr/bin/env bash
case "\${1:-}" in -s) printf 'Darwin\n' ;; *) exec "$REAL_UNAME" "\$@" ;; esac
EOF
  chmod +x "$d20/uname"
  run_cap "$d20" "$ef2" -- --report
  if printf '%s' "$out" | grep -q '^claude-auth: UNMEASURED'; then
    ok "platform: claude-auth is UNMEASURED off Linux (never an [ok])"
  else
    bad "platform: claude-auth off Linux gave: $out"
  fi
  if printf '%s' "$out" | grep -q '^claude-tmux-env: UNMEASURED'; then
    ok "platform: claude-tmux-env is UNMEASURED off Linux, matching its documented contract"
  else
    bad "platform: claude-tmux-env off Linux gave a measured verdict: $out"
  fi
fi

# =====================================================================================
# 19. NO RUN PRINTS A TOKEN-SHAPED VALUE. Asserted over the WHOLE suite transcript, not
#     per case: the property is about every emit path, and a per-case check only covers
#     the paths someone remembered.
# =====================================================================================
if grep -qF -- "$TOK" "$TRANSCRIPT" || grep -qF -- "$TOK_OTHER" "$TRANSCRIPT"; then
  bad "a token value appeared in output — the credential must never be printed"
  grep -nF -- "$TOK" "$TRANSCRIPT" | head -3 | sed 's/'"$TOK"'/<REDACTED-BY-TEST>/g'
  grep -nF -- "$TOK_OTHER" "$TRANSCRIPT" | head -3 | sed 's/'"$TOK_OTHER"'/<REDACTED-BY-TEST>/g'
else
  ok "no token value appeared anywhere in the suite's captured output"
fi

printf '\n== summary ==\npass=%s fail=%s skip=%s\n' "$PASS" "$FAIL" "$SKIP"
# A CASE FLOOR (#3544's lesson): a span-replacing edit that silently deletes cases would
# otherwise report a green tally over a shrunken suite. It is the count of cases that
# ALWAYS run — a floor set to the TOTAL would red whenever a legitimately skippable case
# (the uname stub, the real-tmux isolation case) skips, and a floor that reds on correct
# input is the floor agents learn to delete.
CASE_FLOOR=33
if [ "$((PASS + FAIL))" -lt "$CASE_FLOOR" ]; then
  printf 'FAIL - case floor: %s cases ran, expected at least %s (cases were lost)\n' "$((PASS + FAIL))" "$CASE_FLOOR"
  exit 1
fi
[ "$FAIL" -eq 0 ]
