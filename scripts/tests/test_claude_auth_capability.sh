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
# HOST SAFETY, STATED AS WHAT IS ACTUALLY TRUE. Nothing here reads the real
# /etc/environment or touches the real tmux server: the env-file seam stands in (inert
# without CQLITE_BOOTSTRAP_TEST_MODE=1) and `claude`/`tmux` are recording PATH shims. The
# two cases that run the REAL bootstrap additionally stub `gh`/`sudo`/`roborev`/`cargo` and
# pin the board identity, because bootstrap's OTHER sections otherwise make live GitHub API
# calls, run `sudo`, and (through `roborev check-agents`, which starts the configured agent)
# clone a repository into $HOME — all of which this header once flatly denied, measured on a
# fleet box with recording PATH shims. The one thing still executed for real is a handful of
# READ-ONLY local `git` queries against a directory that is not a repository.
# `gh auth switch` — the one call that would MUTATE operator state, restored only by a trap
# a SIGKILL defeats — is refused by the stub AND asserted never to have been attempted.
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

# THE REAL `uname`, RESOLVED ONCE AND REQUIRED. Both verdicts are Linux-scoped, so every
# shim dir pins `uname -s` to Linux (see mkshim) and the platform-guard case pins it to
# Darwin. A host with no `uname` at all cannot run either pin — and would silently take the
# non-Linux branch in every case, i.e. a suite-wide false red — so it is a NAMED REFUSAL
# rather than a skip.
REAL_UNAME=$(command -v uname 2>/dev/null)
if [ -z "$REAL_UNAME" ]; then
  printf 'test_claude_auth_capability: REFUSING TO RUN (no-uname): the platform pins every case depends on cannot be installed.\n' >&2
  exit 1
fi

# --- shim factory -------------------------------------------------------------------
# mkshim <dir>: a fresh PATH directory holding a `uname` that reports Linux and NOTHING
# ELSE (so `claude`/`tmux` are absent unless a case plants them). `/usr/bin` etc. still
# follow on PATH, so a case that wants a tool ABSENT must plant a refusing stub rather
# than rely on an empty dir.
#
# THE `uname` PIN IS NOT COSMETIC, AND ITS ABSENCE RED 26 OF 48 CASES ON macOS. Both
# verdict functions are Linux-scoped by design (/etc/environment + pam_env is a Linux
# mechanism), so on a Darwin host every `--auth`/`--tmux-env` case below — each of which
# asserts a MEASURED verdict — got UNMEASURED instead. `tooling-tests` is a MANDATORY gate
# component and this repo supports macOS (it ships `gtimeout`/`taskpolicy`/`perf` guards
# for it), so that is a suite that reds on CORRECT INPUT on a supported platform: the
# guard agents learn to waive. The sibling suite already solved this the same way
# (`mk_stub "$dir" uname 'echo Linux'` in test_bootstrap_agent_machine.sh). Pinned HERE
# rather than per case so the platform-guard case's Darwin stub stays the ONLY place the
# platform varies — it overwrites this one in its own shim dir.
mkshim() {
  local d="$1"; rm -rf "$d"; mkdir -p "$d"
  plant_uname_linux "$d"
  printf '%s' "$d"
}
# plant_uname_linux <dir>: `uname -s` says Linux; every other invocation defers to the
# real binary, so nothing else about the host is faked.
# REMOVE FIRST, never write THROUGH the path: MINPATH holds SYMLINKS to the real coreutils
# and `cat >` FOLLOWS a symlink, so writing through it would either fail silently (a
# root-owned target, leaving the case to run against the real `uname`) or TRUNCATE THE REAL
# BINARY. Same hazard, same fix, as mk_stub in test_bootstrap_agent_machine.sh — and the
# same fail-loud check, because a harness that cannot install a pin produces a PASSING case
# that tested nothing.
plant_uname_linux() {
  local d="$1"
  rm -f "$d/uname"
  cat >"$d/uname" <<EOF
#!/usr/bin/env bash
case "\${1:-}" in -s|'') printf 'Linux\n' ;; *) exec $REAL_UNAME "\$@" ;; esac
EOF
  chmod +x "$d/uname"
  if [ -L "$d/uname" ] || [ ! -f "$d/uname" ] || [ ! -x "$d/uname" ] \
     || [ "$("$d/uname" -s 2>/dev/null)" != Linux ]; then
    printf 'test_claude_auth_capability: REFUSING TO RUN (uname-pin-failed): could not install the Linux platform pin at %s/uname.\n' "$d" >&2
    exit 1
  fi
}

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
  # The wording is a REAL rejection shape ("Failed to authenticate"), not a generic error:
  # since #3733's third round only a positively identified rejection earns FAILED, and this
  # case is about REDACTION on that path — a stub whose text no longer reached FAILED would
  # still pass while testing a different branch.
  cat >"$1/claude" <<'EOF'
#!/usr/bin/env bash
printf 'Failed to authenticate: the API key %s was rejected\n' "${CLAUDE_CODE_OAUTH_TOKEN-<unset>}"
exit 1
EOF
  chmod +x "$1/claude"
}
# plant_claude_probe_env <dir>: rc 0, and it reports — CLASSIFIED, never as a value — what
# the probe process actually handed it. Both halves of the isolation argument are named, so
# a case can assert either one without the suite ever printing the secret:
#   saw=persisted | other-token | other-len=N | unset
#   cfgdir=fresh-empty | persisted-dir | inherited-dir | nonexistent | nonempty | unset
# `fresh-empty` is the property the code CLAIMS — a throwaway, empty CLAUDE_CONFIG_DIR that
# is neither the persisted one nor the inherited one — expressed as a property rather than
# as the mktemp template, so a rename of the template does not red a correct probe.
plant_claude_probe_env() {
  cat >"$1/claude" <<EOF
#!/usr/bin/env bash
t="\${CLAUDE_CODE_OAUTH_TOKEN-}"
if [ "\$t" = '$TOK' ]; then printf 'saw=persisted\n'
elif [ "\$t" = '$TOK_OTHER' ]; then printf 'saw=other-token\n'
elif [ -n "\$t" ]; then printf 'saw=other-len=%s\n' "\${#t}"
else printf 'saw=unset\n'; fi
c="\${CLAUDE_CONFIG_DIR-}"
if [ -z "\$c" ]; then printf 'cfgdir=unset\n'
elif [ "\$c" = '$CFGDIR' ]; then printf 'cfgdir=persisted-dir\n'
elif [ "\$c" = '$CFGDIR_OTHER' ]; then printf 'cfgdir=inherited-dir\n'
elif [ ! -d "\$c" ]; then printf 'cfgdir=nonexistent\n'
else
  # Globbing, not \`find\`: it needs no external tool and has no three-valued rc to read
  # two-valued — an empty match here is emptiness, not a failed scan.
  shopt -s nullglob dotglob
  ents=("\$c"/*)
  if [ "\${#ents[@]}" -eq 0 ]; then printf 'cfgdir=fresh-empty\n'; else printf 'cfgdir=nonempty\n'; fi
fi
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
#   probefail   — no server AND the isolated cold-start probe cannot be started
# `setenv` is always accepted and RECORDED — as key plus VALUE LENGTH, never the value, so
# the recording file cannot become the leak this suite forbids.
#
# `new-session` is EMULATED FAITHFULLY: it runs the command through `sh -c`, inheriting the
# environment the stub itself was started with. That is exactly the delivery property the
# cold-start probe measures — whether the environment a would-be server is started from
# reaches a pane — and running the command through `sh -c` rather than a login shell is the
# measured tmux behaviour (fact 5) the probe depends on.
plant_tmux() {
  local d="$1" mode="$2" cfg="${3:-$CFGDIR}"
  cat >"$d/tmux" <<EOF
#!/usr/bin/env bash
log="$d/tmux-calls.log"
# A private -L/-S socket may precede the command word; the real tmux accepts it there and
# the cold-start probe always passes one.
sock=''
while [ "\$#" -gt 0 ]; do
  case "\$1" in
    -L|-S) sock="\$2"; shift 2 ;;
    *) break ;;
  esac
done
case "\$1" in
  new-session)
    shift
    cmd=''
    while [ "\$#" -gt 0 ]; do
      case "\$1" in
        -d) shift ;;
        -s|-e|-n|-c) shift 2 ;;
        *) cmd="\$1"; shift ;;
      esac
    done
    printf 'new-session sock=%s\n' "\$sock" >>"\$log"
    case '$mode' in
      probefail) printf 'error connecting to socket\n' >&2; exit 1 ;;
    esac
    sh -c "\$cmd"
    exit \$? ;;
  kill-server)
    printf 'kill-server sock=%s\n' "\$sock" >>"\$log"
    exit 0 ;;
  show-environment)
    case '$mode' in
      no-server|probefail) printf 'no server running on %s/tmux-1000/default\n' "\${TMPDIR:-/tmp}" >&2; exit 1 ;;
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
# The SAME Linux pin as mkshim: MINPATH is a PATH in its own right, so a case that used it
# without a shim dir in front would fall through to the host's real `uname`.
plant_uname_linux "$MINPATH"
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
#
#     WHAT THIS CASE DOES **NOT** PROVE, stated because a green tally standing in for an
#     unmeasured property is the exact failure this file exists to remove one level down:
#     the env file here carries NO token, so NOT-PERSISTED is decided by the FILE READ
#     before the probe is ever launched. Deleting the `env -u …` scrub leaves this case
#     green. The scrub's own falsifying cases are 1c and 1d below.
# =====================================================================================
run_cap "$d" "$ef" "CLAUDE_CODE_OAUTH_TOKEN=$TOK" -- --auth
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
# 2b. THE `-u BASH_ENV` HALF, AND IT IS THE ONE THAT ACTUALLY BITES. Scrubbing the variable
#     while leaving the mechanism that RE-INJECTS it is not a scrub: a non-interactive bash
#     SOURCES $BASH_ENV at startup, AFTER `env KEY=<persisted>` has run, so an
#     `export CLAUDE_CODE_OAUTH_TOKEN=…` line in that file overrides the value the probe was
#     deliberately handed. #3414 hit exactly this.
#
#     THE FILE MUST EXIST — the earlier version of this case pointed BASH_ENV at a
#     NONEXISTENT path, which sources nothing and therefore exercises nothing. FALSIFIED
#     BOTH WAYS while writing it: delete `-u BASH_ENV` from the probe's `env` and this case
#     reds with `saw=other-token`; restore it and it greens.
be_inject="$tmp/bashenv-inject.sh"
printf 'export CLAUDE_CODE_OAUTH_TOKEN=%s\n' "$TOK_OTHER" >"$be_inject"
d2b=$(mkshim "$tmp/s2b"); plant_claude_probe_env "$d2b"
run_cap "$d2b" "$ef2" "BASH_ENV=$be_inject" -- --auth --show-probe-output
if printf '%s' "$out" | grep -q 'saw=persisted'; then
  ok "claude-auth: a BASH_ENV file that re-exports the credential cannot reach the probe (the -u BASH_ENV scrub)"
else
  bad "claude-auth: BASH_ENV re-injected a credential into the probe: $out"
fi
if printf '%s' "$out" | grep -q '^claude-auth: VERIFIED'; then
  ok "claude-auth: ...and the verdict is still about the PERSISTED value"
else
  bad "claude-auth: the BASH_ENV case did not reach a verdict about the persisted value: $out"
fi
#     `-u ENV` RIDES WITH IT AND IS **NOT** SEPARATELY REACHABLE HERE, stated rather than
#     faked: $ENV is sourced only by an INTERACTIVE POSIX shell (and bash reads it only in
#     posix mode), while every shell this code starts — the `claude` child and the cold
#     probe's `sh -c` pane — is non-interactive. So no fixture can make an `ENV` file change
#     an outcome, and a case that "covers" it would be asserting something already true.
#     It is scrubbed anyway because the cost is one word and the failure mode is silent.

# =====================================================================================
# 2c. THE OTHER HALF OF THE ISOLATION ARGUMENT: A FRESH, EMPTY CLAUDE_CONFIG_DIR. The probe
#     asks whether the PERSISTED TOKEN authenticates, so it must not run against the box's
#     own config directory — a probe inheriting it could pass on session state the token had
#     nothing to do with. That property was printed by the stub and asserted by NOTHING.
#     Here the persisted dir and a DIFFERENT inherited dir both exist, and the probe must
#     receive neither: an empty throwaway.
d2c=$(mkshim "$tmp/s2c"); plant_claude_probe_env "$d2c"
run_cap "$d2c" "$ef2" "CLAUDE_CONFIG_DIR=$CFGDIR_OTHER" -- --auth --show-probe-output
if printf '%s' "$out" | grep -q 'cfgdir=fresh-empty'; then
  ok "claude-auth: the probe runs against a FRESH EMPTY config dir — neither the persisted nor the inherited one"
else
  bad "claude-auth: the probe did not get an isolated config dir: $out"
fi
# ...and it does not survive the run. A leaked throwaway config dir is a real cost on a
# fleet box, and the `mktemp -d`/`rm -rf` pair has no trap between it and the probe.
strayc=$(find "${TMPDIR:-/tmp}" -maxdepth 1 -name 'cqlite-claude-probe.*' -newer "$TRANSCRIPT" 2>/dev/null | head -3)
if [ -z "$strayc" ]; then
  ok "claude-auth: the throwaway config dir is removed, not leaked"
else
  bad "claude-auth: stray probe config dirs survive: $strayc"
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
if printf '%s' "$out" | grep -q '^claude-auth: UNMEASURED'; then
  ok "claude-auth: rc 0 with NO sentinel is not VERIFIED (and is UNMEASURED, not an accusation)"
else
  bad "claude-auth: rc 0 + empty output produced '$out' — rc alone must not certify"
fi

# =====================================================================================
# 5. OUTPUT ALONE IS NOT ENOUGH either — the sentinel with a non-zero rc is a failure.
# =====================================================================================
d5=$(mkshim "$tmp/s5")
plant_claude "$d5" 1 "$SENTINEL"
run_cap "$d5" "$ef2" -- --auth
if printf '%s' "$out" | grep -q '^claude-auth: UNMEASURED'; then
  ok "claude-auth: the sentinel with rc != 0 is not VERIFIED (and names no rejection, so UNMEASURED)"
else
  bad "claude-auth: sentinel + rc 1 produced '$out' — output alone must not certify"
fi

# =====================================================================================
# 5b. AN ARGV ECHO IS NOT AN ANSWER. `grep -qF "$SENTINEL"` over the probe's output cannot
#     distinguish a reply from a repetition of the prompt, so while the sentinel WAS the
#     prompt's last word any stub that printed its own argv passed — and this repo shipped
#     exactly that stub (test_bootstrap_agent_machine.sh's `printf "%s\n" "${*##* }"`).
#     The prompt now asks for a TRANSFORMATION it does not itself contain, so the expected
#     string exists nowhere in the input.
d5b=$(mkshim "$tmp/s5b")
cat >"$d5b/claude" <<'EOF'
#!/usr/bin/env bash
# Echoes the LAST WORD OF ITS OWN ARGV, verbatim. This is a real stub shape, not a
# strawman, and it is what a wedged/degraded CLI does when it parrots its input.
printf '%s\n' "${*##* }"
exit 0
EOF
chmod +x "$d5b/claude"
run_cap "$d5b" "$ef2" -- --auth
if printf '%s' "$out" | grep -q '^claude-auth: UNMEASURED'; then
  ok "claude-auth: a probe that merely ECHOES the prompt does not satisfy the sentinel"
else
  bad "claude-auth: an argv echo was accepted as an authenticated answer: $out"
fi
# ...and the prompt must not CONTAIN the sentinel, or the case above passes by accident.
# Read the two constants at RUNTIME by sourcing the library, never by grepping its source:
# a source-text scan checks a SPELLING, and the defective form spelled it
# `…: $CLAUDE_AUTH_SENTINEL`, which no literal grep sees. Verified by reverting the prompt:
# the grep form stayed green, this form reds.
cap_prompt=$(bash -c '. "$1"; printf "%s" "$CLAUDE_AUTH_PROMPT"' _ "$CAPLIB" 2>/dev/null)
cap_sent=$(bash -c '. "$1"; printf "%s" "$CLAUDE_AUTH_SENTINEL"' _ "$CAPLIB" 2>/dev/null)
if [ -z "$cap_prompt" ] || [ -z "$cap_sent" ]; then
  bad "the probe prompt/sentinel could not be read from the library — no verdict is available"
elif [ "$cap_sent" != "$SENTINEL" ]; then
  bad "the library's sentinel ($cap_sent) is not the one this suite plants ($SENTINEL)"
else
  case "$cap_prompt" in
    *"$cap_sent"*) bad "the probe prompt CONTAINS the sentinel — an argv echo would satisfy it again" ;;
    *) ok "the probe prompt does not contain the sentinel, so an echo of the input cannot produce it" ;;
  esac
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
# 7b. `grep` IS THREE-VALUED AND MUST NOT BE READ TWO-VALUED. It exits 0 on a match, 1 on
#     NO match and >=2 on an ERROR (127 when absent). The shipped `if ! grep -q …; then
#     absent` collapsed "cannot tell" onto the AFFIRMATIVE "no token line here", so a box
#     whose token IS provisioned reported NOT-PERSISTED and bootstrap told the operator to
#     add a line that is already there. This repo lints for exactly that shape
#     (`1699-find-tristate`), and the function's own comment enumerates `unreadable` as a
#     state it distinguishes — which it then lost.
d7b=$(mkshim "$tmp/s7b"); plant_claude_probe_env "$d7b"
cat >"$d7b/grep" <<'EOF'
#!/usr/bin/env bash
printf 'grep: memory exhausted\n' >&2
exit 2
EOF
chmod +x "$d7b/grep"
run_cap "$d7b" "$ef2" -- --auth      # $ef2 DOES carry the token
if printf '%s' "$out" | grep -q '^claude-auth: UNMEASURED'; then
  ok "claude-auth: a grep ERROR is UNMEASURED, never the affirmative 'no token line here'"
else
  bad "claude-auth: a failing grep was read as an absent assignment: $out"
fi

# 7c. A DANGLING SYMLINK IS `unreadable`, NOT `absent-file`. The comment two lines above the
#     check says a symlink is refused rather than followed — but `[ ! -e ]` ran FIRST and a
#     dangling link fails it, so the code answered `absent-file` (NOT-PERSISTED: "provision
#     it") about a path whose target we deliberately refuse to look at. Code and comment
#     have to agree, and the non-permissive answer is the correct one.
d7c=$(mkshim "$tmp/s7c"); plant_claude_probe_env "$d7c"
ln -s "$tmp/no-such-env-file" "$tmp/env-dangling"
run_cap "$d7c" "$tmp/env-dangling" -- --auth
if printf '%s' "$out" | grep -q '^claude-auth: UNMEASURED'; then
  ok "claude-auth: a DANGLING symlink is unreadable (UNMEASURED), not a missing file"
else
  bad "claude-auth: a dangling symlink was classified as an absent file: $out"
fi

# 7d. pam_env STRIPS A LEADING `export `, SO THE PARSER MUST TOO. MEASURED, not reasoned
#     about, through a REAL PAM session (`/etc/pam.d/sudo` carries `pam_env.so readenv=1`,
#     so `sudo env` shows exactly what pam_env delivered from /etc/environment):
#       `export K=v`      -> delivered            `export  K=v` (two spaces) -> NOT delivered
#       `  export K=v`    -> delivered            `export<TAB>K=v`           -> NOT delivered
#       `exportK=v`       -> NOT delivered        `setenv K=v`               -> NOT delivered
#     i.e. exactly the 7-byte literal `export ` after leading whitespace, which is also the
#     one string `strings /usr/lib/x86_64-linux-gnu/security/pam_env.so` carries and what
#     `man 8 pam_env` documents ("The export instruction can be specified for bash
#     compatibility, but will be ignored"). Without this, a box provisioned with an
#     `export`-prefixed line reads NOT-PERSISTED while every real session gets the token.
ef_export="$tmp/env-export"
mkenvfile "$ef_export" "export CLAUDE_CODE_OAUTH_TOKEN=$TOK" "  export CLAUDE_CONFIG_DIR=$CFGDIR"
d7d=$(mkshim "$tmp/s7d"); plant_claude_probe_env "$d7d"
run_cap "$d7d" "$ef_export" -- --auth
if printf '%s' "$out" | grep -q '^claude-auth: VERIFIED'; then
  ok "claude-auth: an 'export '-prefixed assignment is read the way pam_env reads it"
else
  bad "claude-auth: an export-prefixed token line was not seen: $out"
fi
# ...and the non-forms pam_env REJECTS must not be honoured either, or the parser would
# certify a line no session receives — the permissive direction, and the worse one.
ef_notexport="$tmp/env-not-export"
mkenvfile "$ef_notexport" "export  CLAUDE_CODE_OAUTH_TOKEN=$TOK" "setenv CLAUDE_CODE_OAUTH_TOKEN=$TOK" \
  "exportCLAUDE_CODE_OAUTH_TOKEN=$TOK"
d7e=$(mkshim "$tmp/s7e"); plant_claude_probe_env "$d7e"
run_cap "$d7e" "$ef_notexport" -- --auth
if printf '%s' "$out" | grep -q '^claude-auth: NOT-PERSISTED'; then
  ok "claude-auth: the export spellings pam_env REJECTS are not honoured either"
else
  bad "claude-auth: a line pam_env would ignore was accepted as persisted: $out"
fi

# =====================================================================================
# 8-12. THE TMUX DIMENSION — the one that actually failed in the field and that no
#       existing check covers. A pane's environment comes from the SERVER.
# =====================================================================================
# `no-server` is NOT in this loop: a serverless box is no longer a single verdict but a
# whole COLD-START measurement, driven case by case in section 21 below.
for mode_case in \
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
# THE MEASUREMENT'S OWN SUCCESS IS A PRECONDITION. `new_files=$(comm -13 …)` over a FAILED
# find or a FAILED comm yields the empty string, the `while` body never runs, `leaked_files`
# stays empty and the case reports `ok` — a pass derived from a measurement that did not
# happen. `find | sort > f` cannot be rc-checked either (the rc is sort's), so find writes a
# raw file first and each step's status is read on its own line.
# DECLARED SCOPE, because a scan is only as wide as its root: this covers $tmp ONLY. A write
# to /etc/environment.d/ or $HOME would be invisible here. The property that IS asserted is
# that the repair creates no new file under the sandbox carrying the credential.
leak_scan=ok
find "$tmp" -type f >"$tmp/raw-before" 2>/dev/null || leak_scan=find-before-failed
sort "$tmp/raw-before" >"$tmp/files-before" 2>/dev/null || leak_scan=sort-before-failed
[ -s "$tmp/files-before" ] || leak_scan=empty-before
run_cap "$d15" "$ef2" -- --fix-tmux-env
find "$tmp" -type f >"$tmp/raw-after" 2>/dev/null || leak_scan=find-after-failed
sort "$tmp/raw-after" >"$tmp/files-after" 2>/dev/null || leak_scan=sort-after-failed
[ -s "$tmp/files-after" ] || leak_scan=empty-after
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
# is refused on the PRECEDENT of openspec/specs/worker-environment-preflight/spec.md, whose
# "SHALL NOT write the token itself to disk" clause is stated for `$GH_TOKEN` under the
# git-credential requirement — the rule is applied here by analogy, not quoted as if it
# already named this credential.
# Only files the FIX ITSELF created are in scope: the suite's own shims and pam_env
# fixture legitimately embed the planted literal. Diffing before/after is what makes this
# a statement about the CODE rather than about the harness.
new_files=$(comm -13 "$tmp/files-before" "$tmp/files-after") || leak_scan=comm-failed
# THE DIFF MUST BE NON-EMPTY. The repair provably creates at least the tmux stub's call log,
# so an empty diff means the before/after pair did not observe the run — the same vacuity
# one step further in.
scanned=0
leaked_files=''
while IFS= read -r nf; do
  [ -n "$nf" ] || continue
  case "$nf" in "$TRANSCRIPT"|"$tmp/raw-before"|"$tmp/raw-after"|"$tmp/files-before"|"$tmp/files-after") continue ;; esac
  scanned=$((scanned + 1))
  grep -qF -- "$TOK" "$nf" 2>/dev/null && leaked_files="$leaked_files $nf"
done <<EOF
$new_files
EOF
if [ "$leak_scan" != ok ]; then
  bad "--fix-tmux-env: the new-file census could not be taken ($leak_scan) — no verdict is available"
elif [ "$scanned" -eq 0 ]; then
  bad "--fix-tmux-env: the census saw NO new file at all, so it observed nothing (the tmux call log alone should appear)"
elif [ -z "$leaked_files" ]; then
  ok "--fix-tmux-env: wrote the token to NO new file (census: $scanned new files under \$tmp)"
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

# EVERY OTHER SECTION OF BOOTSTRAP IS STUBBED OUT, and this is not tidiness — it was a
# HOST-SAFETY DEFECT. With only `claude`/`tmux` planted, these two invocations ran the REAL
# rest of bootstrap. MEASURED with recording shims on this box: each run made
# `gh auth status`, `gh api graphql`, `gh project view` (live API calls), `sudo -n true`,
# and — via `roborev check-agents` -> codex — a `git ls-remote` plus a
# `git fetch --depth 1 https://github.com/openai/plugins.git` into $HOME. Twice per suite
# run, inside the MANDATORY `tooling-tests` gate component, against a header that claimed
# "No network call is ever made". Worse, on a box whose active gh account differs from
# CQLITE_PROJECT_ACCOUNT with no GH_TOKEN in the environment, bootstrap runs
# `gh auth switch` — real, host-visible mutation restored only by a trap a `kill -9`
# defeats. It was inert here only because this box happens to export GH_TOKEN.
# Same stub set, and the same reasoning, as mk_push_bin in test_bootstrap_agent_machine.sh.
BS_GH_LOG="$tmp/bs-gh-calls.log"; : >"$BS_GH_LOG"
BS_ACCOUNT='cqlite-bootstrap-test'
plant_bootstrap_quiet_stubs() {
  local d="$1" t
  # `gh`: satisfies the auth + board sections offline. It reports the account the run PINS
  # as CQLITE_PROJECT_ACCOUNT, so bootstrap's `gh auth switch` branch is structurally
  # unreachable whatever the host's GH_TOKEN state — and `auth switch` is RECORDED and
  # REFUSED anyway, so the case below can assert it never happened rather than assume it.
  cat >"$d/gh" <<EOF
#!/usr/bin/env bash
printf 'gh %s\n' "\$*" >>'$BS_GH_LOG'
case "\$1" in
  auth)
    case "\$2" in
      status)
        echo "github.com"
        echo "  ✓ Logged in to github.com account $BS_ACCOUNT (keyring)"
        echo "  - Active account: true"
        echo "  - Token scopes: 'gist', 'project', 'read:org', 'repo', 'workflow'"
        exit 0 ;;
      token) echo 'gh-stub-token'; exit 0 ;;
      switch) echo 'stub: refusing to mutate gh state' >&2; exit 1 ;;
    esac
    exit 0 ;;
  project) echo '{"id":"PVT_stub"}'; exit 0 ;;
  api)     echo 'PVT_stub'; exit 0 ;;
esac
exit 0
EOF
  chmod +x "$d/gh"
  # `sudo`: strip its own flags and run the command unprivileged. Without it these runs
  # fall through to the REAL sudo (and, in other sections, the REAL /etc/environment).
  cat >"$d/sudo" <<'EOF'
#!/usr/bin/env bash
while [ "${1:-}" = "-n" ]; do shift; done
if [ "${1:-}" = "-u" ]; then shift 2; fi
exec "$@"
EOF
  chmod +x "$d/sudo"
  # `roborev` is what reached the network: bootstrap runs `roborev check-agents`, roborev
  # runs the configured agent (codex), and codex clones openai/plugins into $HOME.
  for t in roborev codex cargo cargo-nextest sccache mold; do
    printf '#!/usr/bin/env bash\nexit 0\n' >"$d/$t"; chmod +x "$d/$t"
  done
  printf '#!/usr/bin/env bash\ncase "$*" in *stat*) echo "1234567,,cycles" >&2 ;; esac\nexit 0\n' >"$d/perf"
  chmod +x "$d/perf"
}
d18=$(mkshim "$tmp/s18"); plant_claude_probe_env "$d18"; plant_tmux "$d18" complete
plant_bootstrap_quiet_stubs "$d18"
# run_bootstrap <args...> -> $bs_out. The board identity is PINNED so the run cannot vary
# with the host operator's exported CQLITE_PROJECT_* values.
run_bootstrap() {
  PATH="$d18:$PATH" env CQLITE_BOOTSTRAP_TEST_MODE=1 CQLITE_BOOTSTRAP_SKIP_GATE_PIN=1 \
    CQLITE_CLAUDE_AUTH_ENV_FILE="$ef2" HOME="$tmp/home" \
    CQLITE_PROJECT_ACCOUNT="$BS_ACCOUNT" CQLITE_PROJECT_OWNER=pmcfadin CQLITE_PROJECT_NUMBER=1 \
    bash "$bs_root/scripts/bootstrap-agent-machine.sh" "$@" 2>&1
}
bs_out=$(run_bootstrap --skip-smoke --skip-push-probe --skip-claude-auth)
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
bs_out2=$(run_bootstrap --skip-smoke --skip-push-probe)
printf '%s\n' "$bs_out2" >>"$TRANSCRIPT"
if printf '%s' "$bs_out2" | grep -q 'claude-auth: VERIFIED' \
   && printf '%s' "$bs_out2" | grep -q 'claude-tmux-env: VERIFIED'; then
  ok "bootstrap: both verdict lines surface through bootstrap's reporter"
else
  bad "bootstrap: the verdict lines are missing from the run"
  printf '%s\n' "$bs_out2" | sed -n '/Claude credential/,/^$/p'
fi
# HOST SAFETY, ASSERTED RATHER THAN INTENDED: `gh auth switch` mutates the operator's real
# gh state and is restored only by a trap a SIGKILL defeats. The stub refuses it and logs
# every gh call, so this is a measurement of what the two runs above actually did.
if [ -s "$BS_GH_LOG" ] && ! grep -q '^gh auth switch' "$BS_GH_LOG"; then
  ok "bootstrap cases: no 'gh auth switch' was attempted (and gh WAS exercised, so the log is not vacuous)"
else
  bad "bootstrap cases: gh auth switch was attempted, or gh was never called at all: $(cat "$BS_GH_LOG" 2>/dev/null | head -5)"
fi

# CONTRADICTORY INTENTS ARE A USAGE ERROR, not a silent resolution (the --fix-gate-pin rule).
usage_rc=0
# Under the stub PATH too: this run exits at ARGUMENT PARSING, before any section, but a
# host-safety property that holds only because of where a check sits in the file is one
# refactor away from not holding.
usage_out=$(PATH="$d18:$PATH" bash "$bs_root/scripts/bootstrap-agent-machine.sh" --skip-claude-auth --fix-claude-auth 2>&1) || usage_rc=$?
printf '%s\n' "$usage_out" >>"$TRANSCRIPT"
if [ "$usage_rc" = 2 ] && printf '%s' "$usage_out" | grep -q 'contradictory'; then
  ok "bootstrap: --skip-claude-auth beside --fix-claude-auth is a usage error (exit 2)"
else
  bad "bootstrap: contradictory flags resolved silently (rc=$usage_rc): $usage_out"
fi

# =====================================================================================
# 19. `NONEMPTY` IS NOT `CORRECT` — the CLAUDE_CONFIG_DIR half of the tmux verdict.
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
# This is the ONE place the platform varies: every other shim dir pins `uname -s` to Linux
# (mkshim), so this Darwin stub — written into d20 AFTER mkshim, overwriting it — is the
# only Darwin the suite ever sees, on a Linux host and on a macOS host alike.
d20=$(mkshim "$tmp/s20"); plant_claude_probe_env "$d20"; plant_tmux "$d20" complete
cat >"$d20/uname" <<EOF
#!/usr/bin/env bash
case "\${1:-}" in -s|'') printf 'Darwin\n' ;; *) exec "$REAL_UNAME" "\$@" ;; esac
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

# =====================================================================================
# 21. THE COLD-START PROBE — a box with NO tmux server must still be able to PASS.
#     `.agent-ami/profile.yaml` runs bootstrap with --strict at the exact moment a freshly
#     provisioned machine has no tmux server yet, so the old NO-SERVER-always verdict red
#     on this check's PRIMARY use case with no way out (--fix-claude-auth deliberately
#     excludes NO-SERVER). The answerable question there is not "what does the live server
#     hold" but "would a NEWLY created server deliver the credential to a pane" — which is
#     measurable: start a throwaway server on a PRIVATE socket from the environment
#     reconstructed from the PERSISTED source, spawn a pane, and see what it receives.
# =====================================================================================
d21=$(mkshim "$tmp/s21"); plant_tmux "$d21" no-server
run_cap "$d21" "$ef2" -- --tmux-env
if printf '%s' "$out" | grep -q '^claude-tmux-env: VERIFIED'; then
  ok "claude-tmux-env: a box with NO live server PASSES when the persisted environment delivers both variables"
else
  bad "claude-tmux-env: a fresh box with a good persisted environment could not pass: $out"
fi
if [ "$rc" -eq 0 ]; then
  ok "claude-tmux-env: that cold-start VERIFIED exits 0 (so --strict on a fresh box can succeed)"
else
  bad "claude-tmux-env: cold-start VERIFIED exited $rc"
fi
if [ -f "$d21/tmux-calls.log" ] && grep -qE '^kill-server sock=/.*cqlite-tmux-probe\..*/cqlite-authprobe\.sock$' "$d21/tmux-calls.log"; then
  ok "cold-start probe: the throwaway server is killed on its own PRIVATE socket"
else
  bad "cold-start probe: no kill-server on a private socket was recorded: $(cat "$d21/tmux-calls.log" 2>/dev/null)"
fi
# The probe must NEVER touch the host's live server: every tmux call it makes carries the
# private socket, and the only socket-less call is the read-only show-environment probe.
if [ -f "$d21/tmux-calls.log" ] && ! grep -q 'sock=$' "$d21/tmux-calls.log"; then
  ok "cold-start probe: no probe call was made against the DEFAULT tmux socket"
else
  bad "cold-start probe: a call landed on the host's default socket: $(cat "$d21/tmux-calls.log" 2>/dev/null)"
fi

# ...and it does NOT pass when the persisted environment is bad. Two shapes, because the
# remedies differ: nothing to deliver at all, vs. a token but no config dir.
d21b=$(mkshim "$tmp/s21b"); plant_tmux "$d21b" no-server
run_cap "$d21b" "$ef" -- --tmux-env      # $ef carries NO token
if printf '%s' "$out" | grep -q '^claude-tmux-env: COLD-START-MISSING'; then
  ok "claude-tmux-env: COLD-START-MISSING when the persisted source would deliver no token to a new pane"
else
  bad "claude-tmux-env: a tokenless persisted source did not fail the cold-start probe: $out"
fi
if [ "$rc" -ne 0 ]; then
  ok "claude-tmux-env: COLD-START-MISSING exits non-zero"
else
  bad "claude-tmux-env: COLD-START-MISSING exited 0"
fi
d21c=$(mkshim "$tmp/s21c"); plant_tmux "$d21c" no-server
run_cap "$d21c" "$ef_nocfg" -- --tmux-env
if printf '%s' "$out" | grep -q '^claude-tmux-env: COLD-START-INCOMPLETE'; then
  ok "claude-tmux-env: COLD-START-INCOMPLETE when a new pane would get the token but no CLAUDE_CONFIG_DIR"
else
  bad "claude-tmux-env: a config-dir-less persisted source passed the cold-start probe: $out"
fi
d21d=$(mkshim "$tmp/s21d"); plant_tmux "$d21d" no-server
run_cap "$d21d" "$ef_ghost" -- --tmux-env
if printf '%s' "$out" | grep -q '^claude-tmux-env: COLD-START-NODIR'; then
  ok "claude-tmux-env: COLD-START-NODIR when the delivered CLAUDE_CONFIG_DIR does not exist"
else
  bad "claude-tmux-env: a nonexistent delivered config dir passed the cold-start probe: $out"
fi

# NO-SERVER survives ONLY for the case where the isolated probe genuinely could not run,
# and it stays UNMEASURED-class: an unmeasured capability never inherits the permissive
# branch. It is textually distinct from COLD-START-MISSING because the operator actions
# differ — fix the box's tmux vs. provision the credential.
d21e=$(mkshim "$tmp/s21e"); plant_tmux "$d21e" probefail
run_cap "$d21e" "$ef2" -- --tmux-env
if printf '%s' "$out" | grep -q '^claude-tmux-env: NO-SERVER'; then
  ok "claude-tmux-env: NO-SERVER (UNMEASURED-class) only when the isolated probe could not run"
else
  bad "claude-tmux-env: an unrunnable probe did not report NO-SERVER: $out"
fi
if [ "$rc" -ne 0 ]; then
  ok "claude-tmux-env: NO-SERVER still exits non-zero"
else
  bad "claude-tmux-env: NO-SERVER exited 0 — an unmeasured capability must never read as success"
fi
# The INHERITED credential must not be able to answer for the PERSISTED one here either:
# a good token in the ambient environment with nothing persisted is still a fail.
# FALSIFIED: delete `-u "$CLAUDE_AUTH_TOKEN_KEY"` from the cold probe's `env` array and this
# case reds (the pane receives the inherited token, whose length does not match the empty
# persisted one). That flag is THE MECHANISM here, not belt, because the re-supplying
# assignment is CONDITIONAL — with nothing persisted, nothing overrides the inherited value.
d21f=$(mkshim "$tmp/s21f"); plant_tmux "$d21f" no-server
run_cap "$d21f" "$ef" "CLAUDE_CODE_OAUTH_TOKEN=$TOK" "CLAUDE_CONFIG_DIR=$CFGDIR" -- --tmux-env
if printf '%s' "$out" | grep -q '^claude-tmux-env: COLD-START-MISSING'; then
  ok "cold-start probe: an INHERITED credential cannot satisfy the cold-start question (the scrub)"
else
  bad "cold-start probe: the inherited environment leaked into the probe: $out"
fi

# =====================================================================================
# 22. THE COLD-START PROBE AGAINST THE REAL tmux. The shim cases above pin the wiring and
#     the verdicts; only real tmux can show that the mechanism WORKS and that it leaves
#     nothing behind. TMUX_TMPDIR points the whole case at a private, empty socket
#     directory, so it can neither see nor touch this host's live server.
# =====================================================================================
if ! command -v tmux >/dev/null 2>&1; then
  skip "cold-start probe against the real tmux" "no tmux on this host"
else
  TT="$tmp/tmux-tmpdir"; rm -rf "$TT"; mkdir -p "$TT"; chmod 700 "$TT"
  d22=$(mkshim "$tmp/s22")
  # TMUX/TMUX_PANE ARE UNSET HERE, AND THAT IS NOT COSMETIC. A tmux CLIENT started inside a
  # pane connects to the server named in $TMUX and IGNORES TMUX_TMPDIR — measured while
  # writing this case: the run read THIS HOST'S live server and reported SERVER-STALE. So
  # the isolation of this case depends on both, and it is invoked directly rather than
  # through run_cap because `env` stops parsing options at the first assignment.
  rc=0
  out=$(PATH="$d22:$PATH" env -u TMUX -u TMUX_PANE \
        CQLITE_BOOTSTRAP_TEST_MODE=1 CQLITE_CLAUDE_AUTH_ENV_FILE="$ef2" TMUX_TMPDIR="$TT" \
        bash "$CAPLIB" --tmux-env 2>&1) || rc=$?
  printf '%s\n' "$out" >>"$TRANSCRIPT"
  if printf '%s' "$out" | grep -q '^claude-tmux-env: VERIFIED'; then
    ok "cold-start probe: REAL tmux, no live server, good persisted environment -> VERIFIED"
  else
    bad "cold-start probe: the real-tmux cold path did not verify: $out"
  fi
  # NOTHING was created in the shared socket directory at all — the probe used a socket
  # inside its own working directory, so it can neither collide with nor outlive anything.
  leftover=$(find "$TT" -type s 2>/dev/null | head -5)
  if [ -z "$leftover" ]; then
    ok "cold-start probe: REAL tmux left no server socket in the shared socket directory"
  else
    bad "cold-start probe: a tmux socket survived the probe: $leftover"
  fi
  stray=$(find "${TMPDIR:-/tmp}" -maxdepth 1 -name 'cqlite-tmux-probe.*' -newer "$TRANSCRIPT" 2>/dev/null | head -3)
  if [ -z "$stray" ]; then
    ok "cold-start probe: no working directory was left in the temp root"
  else
    bad "cold-start probe: stray working directories: $stray"
  fi
fi

# =====================================================================================
# 25. OUTPUT LARGER THAN THE PIPE BUFFER MUST NOT INVERT A SUCCESSFUL MATCH.
#     `set -o pipefail` is on, and `grep -q`/`grep -m1` EXIT ON THE FIRST MATCH. When the
#     producer still has more than a pipe buffer's worth (64 KiB on Linux) to write it
#     takes SIGPIPE and dies 141, so the PIPELINE reports failure — and a SUCCESSFUL match
#     reads as unsuccessful. Measured: a 200 KB blob whose sentinel is on line 1 gives
#     `grep -qF` rc 141, and the `grep -m1` command substitution likewise, so its `|| return
#     0` fires and the key is reported ABSENT while it is plainly present.
#     Both consequences are wrong in the DANGEROUS direction: a working credential reads as
#     unmeasured/rejected, and a correctly seeded tmux server reads as SERVER-MISSING —
#     whose remedy is to overwrite the very value that is already right.
#     `tmux show-environment -g` on a heavily populated server, and a `claude` run that
#     prints a long preamble, are both ordinary.
#     THE FIX IS TO REMOVE THE PIPE, not to widen the buffer: every match is now a bash
#     builtin (`case`/`[[ =~ ]]`/a line walk over the string), so there is no second process
#     to lose a race with.
# =====================================================================================
# ~200 KB, comfortably past the 64 KiB Linux pipe buffer, generated without assuming any
# particular tool: bash's own printf padding.
BULK=$(printf '%0.sx' $(seq 1 2000))
plant_claude_bulky() { # rc 0, sentinel FIRST, then far more output than a pipe holds
  cat >"$1/claude" <<EOF
#!/usr/bin/env bash
printf '%s\n' '$SENTINEL'
i=0
while [ "\$i" -lt 100 ]; do printf '%s\n' '$BULK'; i=\$((i + 1)); done
exit 0
EOF
  chmod +x "$1/claude"
}
d25=$(mkshim "$tmp/s25"); plant_claude_bulky "$d25"
run_cap "$d25" "$ef2" -- --auth
if printf '%s' "$out" | grep -q '^claude-auth: VERIFIED'; then
  ok "claude-auth: a sentinel on line 1 of 200 KB of output still VERIFIES (no SIGPIPE inversion)"
else
  bad "claude-auth: a large probe output inverted a successful sentinel match: $out"
fi
# The tmux half: the token is the FIRST line, so an early-exiting matcher kills the
# producer with ~200 KB still to write.
plant_tmux_bulky() {
  local d="$1"
  cat >"$d/tmux" <<EOF
#!/usr/bin/env bash
log="$d/tmux-calls.log"
while [ "\$#" -gt 0 ]; do case "\$1" in -L|-S) shift 2 ;; *) break ;; esac; done
case "\$1" in
  show-environment)
    printf 'CLAUDE_CODE_OAUTH_TOKEN=%s\n' '$TOK'
    i=0
    while [ "\$i" -lt 100 ]; do printf 'PADDING_%s=%s\n' "\$i" '$BULK'; i=\$((i + 1)); done
    printf 'CLAUDE_CONFIG_DIR=%s\n' '$CFGDIR'
    exit 0 ;;
esac
exit 0
EOF
  chmod +x "$d/tmux"
}
# ...and the replacement must not trade a wrong answer for an unusable one. The first fix
# here walked the text by SLICING THE REMAINDER, which is quadratic: measured at 41 SECONDS
# for a 5000-variable environment — on exactly the heavily populated server this section is
# about. The lookup now reads the text through a here-string redirect (the loop stays in
# this shell; a piped `while read` would discard its writes) and the same case takes ~60 ms.
# THE BOUND BELOW IS A LIVENESS CEILING, NOT A PERFORMANCE ASSERT: it is ~500x the measured
# cost, so it cannot red on a loaded box, and what it catches is a return to an algorithm
# that does not finish — the failure mode, not a regression in milliseconds.
d25c=$(mkshim "$tmp/s25c")
cat >"$d25c/tmux" <<EOF
#!/usr/bin/env bash
while [ "\$#" -gt 0 ]; do case "\$1" in -L|-S) shift 2 ;; *) break ;; esac; done
case "\$1" in
  show-environment)
    i=0
    while [ "\$i" -lt 5000 ]; do printf 'VAR_%s=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n' "\$i"; i=\$((i + 1)); done
    printf 'CLAUDE_CODE_OAUTH_TOKEN=%s\n' '$TOK'
    printf 'CLAUDE_CONFIG_DIR=%s\n' '$CFGDIR'
    exit 0 ;;
esac
exit 0
EOF
chmod +x "$d25c/tmux"
many_start=$(date +%s)
run_cap "$d25c" "$ef2" -- --tmux-env
many_elapsed=$(( $(date +%s) - many_start ))
if printf '%s' "$out" | grep -q '^claude-tmux-env: VERIFIED' && [ "$many_elapsed" -lt 30 ]; then
  ok "claude-tmux-env: a 5000-variable server environment is read correctly and FINISHES (${many_elapsed}s)"
else
  bad "claude-tmux-env: a 5000-variable server environment was misread or did not finish (${many_elapsed}s): $out"
fi
d25b=$(mkshim "$tmp/s25b"); plant_tmux_bulky "$d25b"
run_cap "$d25b" "$ef2" -- --tmux-env
if printf '%s' "$out" | grep -q '^claude-tmux-env: VERIFIED'; then
  ok "claude-tmux-env: a correctly seeded server with 200 KB of environment still VERIFIES"
else
  bad "claude-tmux-env: a large show-environment inverted the key lookup: $out"
fi

# =====================================================================================
# 26. SHELL TRACING MUST NOT LEAK THE CREDENTIAL.
#     `bash -x` prints every expanded assignment and every command's ARGV *before* the
#     command runs — which is before the redaction boundary can see anything. Measured on
#     the shipped code: `bash -x scripts/claude-auth-capability.sh --auth` printed the
#     token NINE times (the `[ -n "$CLAUDE_AUTH_SECRET" ]` test, the `env KEY=<token> …
#     claude -p` argv, the `tmux setenv -g KEY <token>` argv, …).
#     THIS IS AN ACCIDENT ROUTE, NOT A HOSTILE-INVOKER ONE — which is what makes it a
#     defect under this repo's triage rule: an operator debugging a failing preflight
#     reaches for `bash -x` precisely BECAUSE the credential check is what is failing, and
#     CI harnesses set `set -x` wholesale. The owner's ruling on this issue is that the
#     token is never printed anywhere, and "anywhere" includes a traced run.
#     Every entry point that reads or consumes the secret now suppresses xtrace for its
#     duration and RESTORES the caller's setting (depth-counted, so nesting is safe).
# =====================================================================================
d26=$(mkshim "$tmp/s26"); plant_claude_probe_env "$d26"; plant_tmux "$d26" stale
for traced_mode in --auth --tmux-env --report --fix-tmux-env; do
  traced_rc=0
  traced_out=$(PATH="$d26:$PATH" env CQLITE_BOOTSTRAP_TEST_MODE=1 \
        CQLITE_CLAUDE_AUTH_ENV_FILE="$ef2" \
        bash -x "$CAPLIB" "$traced_mode" 2>&1) || traced_rc=$?
  # NOT appended to $TRANSCRIPT verbatim: a traced run is thousands of lines and, when it
  # leaks, the leak would be reported twice with no extra information. The census below is
  # the verdict; a count is printed instead of the text so a failure names the scale
  # without reprinting the secret.
  traced_hits=$(printf '%s\n' "$traced_out" | grep -cF -- "$TOK")
  if [ "$traced_hits" = 0 ]; then
    ok "bash -x $traced_mode: the credential appears nowhere in a TRACED run"
  else
    bad "bash -x $traced_mode: the credential leaked $traced_hits time(s) under shell tracing"
  fi
done
# ...and the suppression must be a LOAN, not a seizure: this file is SOURCED by bootstrap,
# so a caller that asked for tracing must still have it afterwards. Asserted through the
# real sourcing path rather than by reading the code.
cat >"$tmp/xtrace-restore.sh" <<XEOF
#!/usr/bin/env bash
set -uo pipefail
. "$CAPLIB"
set -x
claude_auth_verdict_into xr_v xr_d 2>/dev/null
# NO \`set +x\` of our own before the probe: turning it off here would answer the
# question the wrong way round. The trace itself goes to stderr, which the caller drops.
case "\$-" in *x*) printf 'XTRACE-STILL-ON\n' ;; *) printf 'XTRACE-LOST\n' ;; esac
XEOF
xr_out=$(PATH="$d26:$PATH" env CQLITE_BOOTSTRAP_TEST_MODE=1 CQLITE_CLAUDE_AUTH_ENV_FILE="$ef2" \
         bash "$tmp/xtrace-restore.sh" 2>/dev/null)
if printf '%s' "$xr_out" | grep -q 'XTRACE-STILL-ON'; then
  ok "xtrace suppression is restored to the caller's setting (a sourced library must not seize it)"
else
  bad "xtrace suppression swallowed the caller's 'set -x': $xr_out"
fi

# =====================================================================================
# 27. STRUCTURAL: EVERY LIBRARY FUNCTION BOOTSTRAP REACHES IS INSIDE THE TRACE BOUNDARY.
#     Section 26 proves the suppression works on today's four entry points. It cannot see
#     a FIFTH one added later and called from bootstrap without a wrapper — which would
#     re-open the leak for that path only, silently, with section 26 still green. So the
#     containment is asserted the way it is stated: the set of library functions bootstrap
#     references must be a SUBSET of the wrapped set, and BOTH sets are DERIVED from the
#     committed sources at run time rather than listed here (a curated list is one more
#     thing to forget).
# =====================================================================================
# The wrapped set: a public name exists as a wrapper iff the file defines <name>__untraced
# at column zero. Derived, never curated.
xtrace_wrapped=$(grep -oE '^[a-z_]+__untraced' "$CAPLIB" | sed 's/__untraced$//' | sort -u)
# library_refs <file>: every claude_auth_*/claude_tmux_* identifier the file names, outside
# whole-line comments. Lowercase by construction, so the CLAUDE_AUTH_* variables are not
# in scope; the `__untraced` implementations are skipped because naming one is naming the
# inside of the boundary, not crossing it.
unwrapped_refs() {
  local f="$1" id
  sed -e 's/^[[:space:]]*#.*$//' "$f" \
    | grep -oE 'claude_(auth|tmux)_[a-z0-9_]+' | sort -u \
    | while IFS= read -r id; do
        case "$id" in *__untraced) continue ;; esac
        printf '%s\n' "$xtrace_wrapped" | grep -qx -- "$id" || printf '%s\n' "$id"
      done
}
if [ -z "$xtrace_wrapped" ]; then
  bad "trace-boundary guard: the wrapped set could not be derived from $CAPLIB (no verdict)"
else
  ok "trace-boundary guard: the wrapped set was derived from the library source ($(printf '%s\n' "$xtrace_wrapped" | grep -c .) names)"
fi
# POSITIVE CONTROL: a caller naming a function that is NOT wrapped must be reported. The
# needle is an internal helper that really exists and really is not a wrapper, so the
# control cannot pass by naming something the pattern would miss anyway.
ctl2="$tmp/fake-bootstrap.sh"
{
  printf '%s\n' '#!/usr/bin/env bash'
  printf '%s\n' '. "$REPO_ROOT/scripts/claude-auth-capability.sh"'
  printf '%s\n' 'claude_auth_verdict_into V D'
  printf '%s\n' 'claude_tmux_show_key_into A B "$text" KEY'
} >"$ctl2"
ctl2_hits=$(unwrapped_refs "$ctl2")
if [ "$ctl2_hits" = 'claude_tmux_show_key_into' ]; then
  ok "trace-boundary guard: an unwrapped library call from a caller is reported (and only it)"
else
  bad "trace-boundary guard: the control did not isolate the unwrapped call: [$ctl2_hits]"
fi
bs_unwrapped=$(unwrapped_refs "$BOOTSTRAP")
if [ -z "$bs_unwrapped" ]; then
  ok "trace-boundary guard: every library function bootstrap calls is inside the trace boundary"
else
  bad "trace-boundary guard: bootstrap calls library functions that do not suppress xtrace: $bs_unwrapped"
fi

# =====================================================================================
# 28. THE BOUND MUST BE HARD, OR THERE MUST BE NO PROBE.
#     The resolver used to KEEP a `timeout` that failed the `--kill-after` probe and run
#     with a SIGTERM-ONLY bound, so a `claude` that ignores SIGTERM ran unbounded and hung
#     the provisioning entry point. Measured on this box with a TERM-ignoring child:
#       timeout 2 <child>                 -> rc 124 after THIRTY seconds (the child's own
#                                            lifetime; the "bound" bounded nothing)
#       timeout --kill-after=2 2 <child>  -> rc 137 after 4 seconds
#     CLAUDE.md's gate doctrine takes the opposite line for exactly this case: where the
#     probe cannot be BOUNDED the probe is not run at all, because a missing capability
#     must not inherit the permissive branch. So the resolver now requires an escalating
#     bound in one of its two spellings (`--kill-after=` GNU, `-k` BusyBox/GNU-short) and
#     otherwise REFUSES, leaving the verdict UNMEASURED with the cause named.
# =====================================================================================
# (a) THE BOUND ACTUALLY TERMINATES A TERM-IGNORING CHILD. Driven through the real probe
#     path — the library is sourced and CLAUDE_AUTH_PROBE_BOUND lowered — so this measures
#     the shipped invocation, not a re-derived one. `exec` is used deliberately: a signal
#     set to SIG_IGN stays ignored across execve, so the process the bound must kill really
#     is one that discards SIGTERM, and it holds no pipe to this suite (a child that did
#     would keep the capture open long after its parent was killed, and the case would time
#     out for the wrong reason).
d28=$(mkshim "$tmp/s28")
cat >"$d28/claude" <<'EOF'
#!/usr/bin/env bash
trap '' TERM
exec sleep 45 >/dev/null 2>&1
EOF
chmod +x "$d28/claude"
cat >"$tmp/hardbound.sh" <<HBEOF
#!/usr/bin/env bash
set -uo pipefail
. "$CAPLIB"
CLAUDE_AUTH_PROBE_BOUND=1
claude_auth_verdict_into hb_v hb_d
printf 'verdict=%s\n' "\$hb_v"
HBEOF
hb_start=$(date +%s)
hb_out=$(PATH="$d28:$PATH" env CQLITE_BOOTSTRAP_TEST_MODE=1 CQLITE_CLAUDE_AUTH_ENV_FILE="$ef2" \
         bash "$tmp/hardbound.sh" 2>&1) || true
hb_elapsed=$(( $(date +%s) - hb_start ))
printf '%s\n' "$hb_out" >>"$TRANSCRIPT"
# 20s is the ceiling for a 1s bound + a 5s kill-after on a loaded box; the UNBOUNDED
# failure this pins takes the child's full 45s, so the two are not close.
if [ "$hb_elapsed" -lt 20 ] && printf '%s' "$hb_out" | grep -q '^verdict=UNMEASURED'; then
  ok "the probe bound KILLS a SIGTERM-ignoring child (${hb_elapsed}s, verdict UNMEASURED)"
else
  bad "the probe bound did not terminate a SIGTERM-ignoring child in time (${hb_elapsed}s): $hb_out"
fi
# (b) A `timeout` WITH NO ESCALATION IS NOT A BOUND, so the probe must not run at all.
#     Both spellings are refused by the shim, and `gtimeout` is planted too — the resolver
#     tries it second, and a host that happened to have one would otherwise rescue the case
#     and hide the defect.
d28b=$(mkshim "$tmp/s28b")
CLAUDE_RAN_MARKER="$tmp/s28b-claude-ran"
rm -f "$CLAUDE_RAN_MARKER"
for tname in timeout gtimeout; do
  cat >"$d28b/$tname" <<EOF
#!/usr/bin/env bash
# A \`timeout\` that knows no escalation flag: it REJECTS both spellings and otherwise
# behaves (BusyBox's older form). Rejecting is what the resolver must notice.
case "\${1:-}" in
  --kill-after*|-k) printf '%s: unrecognized option\n' "\$0" >&2; exit 125 ;;
esac
shift   # the duration; this stub never enforces one
exec "\$@"
EOF
  chmod +x "$d28b/$tname"
done
cat >"$d28b/claude" <<EOF
#!/usr/bin/env bash
: >'$CLAUDE_RAN_MARKER'
printf '%s\n' '$SENTINEL'
exit 0
EOF
chmod +x "$d28b/claude"
run_cap "$d28b" "$ef2" -- --auth
if printf '%s' "$out" | grep -q '^claude-auth: UNMEASURED'; then
  ok "claude-auth: UNMEASURED when no timeout on PATH can enforce a HARD bound"
else
  bad "claude-auth: a SIGTERM-only timeout was accepted as a bound: $out"
fi
if [ ! -e "$CLAUDE_RAN_MARKER" ]; then
  ok "claude-auth: the probe is NOT RUN when the bound cannot be enforced (a would-be VERIFIED stub was never invoked)"
else
  bad "claude-auth: the unbounded probe RAN anyway — a missing capability inherited the permissive branch"
fi
if printf '%s' "$out" | grep -qi 'kill\|hard'; then
  ok "claude-auth: the refusal NAMES the missing hard bound rather than reporting a generic absence"
else
  bad "claude-auth: the refusal does not say which capability was missing: $out"
fi
# The tmux cold-start probe shares the resolver, so it refuses on the same ground.
d28c=$(mkshim "$tmp/s28c"); plant_tmux "$d28c" no-server
for tname in timeout gtimeout; do cp "$d28b/$tname" "$d28c/$tname"; done
run_cap "$d28c" "$ef2" -- --tmux-env
if printf '%s' "$out" | grep -q '^claude-tmux-env: NO-SERVER'; then
  ok "claude-tmux-env: the cold-start probe refuses on the same unenforceable bound (NO-SERVER, UNMEASURED-class)"
else
  bad "claude-tmux-env: the cold-start probe ran without an enforceable bound: $out"
fi

# =====================================================================================
# 29. `FAILED` IS AN ACCUSATION ABOUT A CREDENTIAL, SO IT MUST BE EARNED.
#     Every nonzero probe result that was not one narrow network shape used to be
#     classified FAILED — and FAILED's remedy is "replace the VALUE ... bootstrap never
#     rewrites it". A rate limit, an API outage, a quota error, a CLI crash and a trust
#     prompt are none of them evidence that the credential was rejected, so that is a
#     confident, wrong, ACTIONABLE instruction to throw away a working token. It is this
#     issue's own history repeating: a measurement of something adjacent reported as the
#     thing itself.
#     The rule applied is the doctrine one, verbatim: where the sole oracle could not be
#     consulted the verdict is non-passing and its text names what was unverifiable — it
#     must not become an affirmative negative. Nothing is weakened by the move: UNMEASURED
#     is already non-passing, exits non-zero, and withholds "All checks green" under
#     --strict exactly as FAILED does. Only the operator's next action differs, which is
#     the entire point.
# =====================================================================================
# claude_auth_case <name> <shim-dir-suffix> <rc> <text> <expected-verdict>
claude_auth_case() {
  local nm="$1" sfx="$2" prc="$3" text="$4" want="$5" dd
  dd=$(mkshim "$tmp/$sfx"); plant_claude "$dd" "$prc" "$text"
  run_cap "$dd" "$ef2" -- --auth
  if printf '%s' "$out" | grep -q "^claude-auth: $want"; then
    ok "claude-auth: $nm -> $want"
  else
    bad "claude-auth: $nm expected $want, got: $out"
  fi
}
# Rate limiting and an outage prove nothing about the credential.
claude_auth_case 'a rate-limit refusal (429)' s29a 1 \
  'API Error: 429 {"type":"error","error":{"type":"rate_limit_error","message":"Number of requests has exceeded your rate limit"}}' UNMEASURED
claude_auth_case 'an API outage (503)' s29b 1 \
  'API Error: 503 Service Unavailable — upstream connect error' UNMEASURED
claude_auth_case 'an overloaded upstream (529)' s29c 1 \
  'API Error: 529 {"type":"overloaded_error","message":"Overloaded"}' UNMEASURED
claude_auth_case 'a quota/credit exhaustion' s29d 1 \
  'Your credit balance is too low to access the Anthropic API' UNMEASURED
claude_auth_case 'a CLI crash with no diagnosis' s29e 2 \
  'node:internal/errors: TypeError: Cannot read properties of undefined' UNMEASURED
# ...and the shapes that DO name a credential rejection still earn FAILED, or the change
# would have removed the verdict rather than narrowed it.
claude_auth_case 'the fleet-measured rejection wording' s29f 1 \
  'Failed to authenticate: OAuth session expired and could not be refreshed' FAILED
claude_auth_case 'an invalid API key' s29g 1 \
  'API Error: 401 {"type":"error","error":{"type":"authentication_error","message":"invalid x-api-key"}}' FAILED
claude_auth_case 'an explicit re-login demand' s29h 1 \
  'Invalid API key · Please run /login' FAILED
# A 401 must not be matched inside a longer number — the guard against a matcher that
# earns an accusation from a coincidence.
claude_auth_case 'a 401 embedded in a larger number is NOT a rejection' s29i 1 \
  'API Error: 1401337 requests queued upstream' UNMEASURED

# =====================================================================================
# 30. `--yes` MUST NOT SEED A KNOWN-BAD CREDENTIAL INTO A WORKING tmux SERVER.
#     The repair was gated on the TMUX verdict alone and never consulted `claude-auth:`.
#     So on a box whose PERSISTED token is FAILED (or UNMEASURED) while the RUNNING server
#     holds a working one, `--yes` overwrote the working value with the broken one and every
#     lane spawned afterwards failed to authenticate. The repair BROKE A WORKING BOX — the
#     exact inverse of what this section exists to do — and `.agent-ami/profile.yaml` runs
#     bootstrap unattended, so nobody was watching.
#     SERVER-STALE is precisely the state in which this fires: "the server's token DIFFERS
#     from the persisted one" is reported the same way whether the server's copy is the old
#     broken one or the only working one on the box, and nothing in that verdict can tell
#     them apart. What CAN tell them apart is the other line, which was already measured
#     three lines earlier and simply not read.
#     Seeding therefore requires claude-auth: VERIFIED. The hand-run
#     `claude-auth-capability.sh --fix-tmux-env` stays ungated as the deliberate override,
#     so the refusal costs an operator nothing it does not name.
# =====================================================================================
# run_bootstrap_in <shimdir> <envfile> <args...>: the same pinned, offline invocation as
# section 18, parameterised so a case can vary the planted verdicts.
run_bootstrap_in() {
  local sd="$1" evf="$2"; shift 2
  PATH="$sd:$PATH" env CQLITE_BOOTSTRAP_TEST_MODE=1 CQLITE_BOOTSTRAP_SKIP_GATE_PIN=1 \
    CQLITE_CLAUDE_AUTH_ENV_FILE="$evf" HOME="$tmp/home" \
    CQLITE_PROJECT_ACCOUNT="$BS_ACCOUNT" CQLITE_PROJECT_OWNER=pmcfadin CQLITE_PROJECT_NUMBER=1 \
    bash "$bs_root/scripts/bootstrap-agent-machine.sh" "$@" 2>&1
}
# plant_tmux_stateful <dir> <initial-token> <cfg>: a server whose environment CHANGES when
# it is seeded, so the re-measure after a repair is a real observation rather than a replay
# of the planted state. The happy path below needs it; a fixed-mode stub could only ever
# show that seeding was ATTEMPTED.
plant_tmux_stateful() {
  local d="$1" itok="$2" icfg="$3" st="$1/tmux-state"
  printf 'CLAUDE_CODE_OAUTH_TOKEN=%s\nCLAUDE_CONFIG_DIR=%s\n' "$itok" "$icfg" >"$st"
  cat >"$d/tmux" <<EOF
#!/usr/bin/env bash
log="$d/tmux-calls.log"; st='$st'
while [ "\$#" -gt 0 ]; do case "\$1" in -L|-S) shift 2 ;; *) break ;; esac; done
case "\$1" in
  show-environment) cat "\$st"; exit 0 ;;
  set-environment|setenv)
    shift
    key=''; val=''
    while [ "\$#" -gt 0 ]; do
      case "\$1" in -*) ;; *) if [ -z "\$key" ]; then key="\$1"; else val="\$1"; fi ;; esac
      shift
    done
    printf 'setenv %s len=%s\n' "\$key" "\${#val}" >>"\$log"
    { grep -v "^\$key=" "\$st" || true; } >"\$st.new"
    mv "\$st.new" "\$st"
    printf '%s=%s\n' "\$key" "\$val" >>"\$st"
    exit 0 ;;
esac
exit 0
EOF
  chmod +x "$d/tmux"
}
# (a) THE HARM CASE. Persisted credential REJECTED, running server holds a different one,
#     --yes. Nothing may be written to the server.
d30a=$(mkshim "$tmp/s30a"); plant_bootstrap_quiet_stubs "$d30a"
plant_claude "$d30a" 1 'Failed to authenticate: OAuth session expired and could not be refreshed'
plant_tmux_stateful "$d30a" "$TOK_OTHER" "$CFGDIR"
bs30a=$(run_bootstrap_in "$d30a" "$ef2" --skip-smoke --skip-push-probe --yes)
printf '%s\n' "$bs30a" >>"$TRANSCRIPT"
if printf '%s' "$bs30a" | grep -q 'claude-auth: FAILED'; then
  ok "seed gate: the harm case really does present a REJECTED persisted credential"
else
  bad "seed gate: the fixture did not produce claude-auth: FAILED, so the case tests nothing: $(printf '%s' "$bs30a" | grep -c .) lines"
fi
if [ ! -f "$d30a/tmux-calls.log" ] || ! grep -q '^setenv ' "$d30a/tmux-calls.log"; then
  ok "seed gate: --yes performs NO seed when the persisted credential is not VERIFIED"
else
  bad "seed gate: --yes seeded a non-VERIFIED credential into the running server: $(cat "$d30a/tmux-calls.log")"
fi
if printf '%s' "$bs30a" | grep -qi 'REFUS.*seed\|seed.*REFUS'; then
  ok "seed gate: the refusal is LOUD and names the precondition"
else
  bad "seed gate: --yes declined to seed SILENTLY, which reads as 'nothing was wrong': $(printf '%s' "$bs30a" | sed -n '/Claude credential/,/^$/p' | head -12)"
fi
if printf '%s' "$bs30a" | grep -q 'claude-tmux-env: SERVER-STALE'; then
  ok "seed gate: the tmux verdict is REPORTED UNCHANGED (the run neither repaired nor hid it)"
else
  bad "seed gate: the tmux verdict was not left as found: $(printf '%s' "$bs30a" | grep 'claude-tmux-env:')"
fi
# (b) THE HAPPY PATH STILL SEEDS, and is re-measured rather than asserted. Without this the
#     fix above could have been "never seed", which passes (a) and helps nobody.
d30b=$(mkshim "$tmp/s30b"); plant_bootstrap_quiet_stubs "$d30b"
plant_claude_probe_env "$d30b"
plant_tmux_stateful "$d30b" "$TOK_OTHER" "$CFGDIR"
bs30b=$(run_bootstrap_in "$d30b" "$ef2" --skip-smoke --skip-push-probe --yes)
printf '%s\n' "$bs30b" >>"$TRANSCRIPT"
if [ -f "$d30b/tmux-calls.log" ] && grep -q '^setenv CLAUDE_CODE_OAUTH_TOKEN len=' "$d30b/tmux-calls.log"; then
  ok "seed gate: a VERIFIED persisted credential IS still seeded by --yes"
else
  bad "seed gate: the happy path stopped seeding — the gate is a blanket refusal: $(cat "$d30b/tmux-calls.log" 2>/dev/null)"
fi
if printf '%s' "$bs30b" | grep -q 'claude-tmux-env: VERIFIED'; then
  ok "seed gate: the repair is RE-MEASURED against the changed server and reaches VERIFIED"
else
  bad "seed gate: the seeded server did not re-measure VERIFIED: $(printf '%s' "$bs30b" | grep 'claude-tmux-env:')"
fi

# =====================================================================================
# 24. STRUCTURAL: NO HOST- OR SESSION-SPECIFIC ABSOLUTE PATH LITERAL, ANYWHERE.
#     `tooling-tests` is a MANDATORY gate component, so a path that exists on ONE box —
#     or in ONE agent session — makes the gate host-dependent, and if that directory
#     happens to exist it executes uncontrolled content ahead of everything else. The
#     shipped instance was a recording-shim directory left behind by a measurement round:
#       PATH="$d18:/tmp/claude-1000/<session-uuid>/scratchpad/rec:$PATH"
#     inside `run_bootstrap`, i.e. a PATH entry naming one agent session's scratchpad in
#     the invocation of the REAL bootstrap. It is a CLASS — a measurement seam left behind
#     — not a line, so it is asserted structurally over BOTH files rather than by
#     remembering to delete the one that was found.
#
#     THE RULE, stated so it can be obeyed: every path this suite uses must be derived at
#     RUN TIME — from `mktemp` (`$tmp/...`), from `$REPO_ROOT`, or from `command -v` —
#     never written as a literal rooted at a session/host-specific prefix. The ONE
#     permitted spelling of such a prefix is the `${TMPDIR:-/tmp}` default idiom, which is
#     a FALLBACK ROOT handed to `mktemp`, not a path: every temp path here goes through it.
#     Whole-line comments are excluded — this paragraph names the defect, and a guard that
#     reds on the text describing it is the guard agents learn to delete (the same
#     exclusion, for the same reason, as the eval guard in section 23b).
# =====================================================================================
# scan_abs_path_literals <file>: prints `<lineno>:<text>` for every violating line. Blanks
# whole-line comments IN PLACE (never deletes them) so the reported line numbers are the
# file's own, and strips the permitted `${TMPDIR:-/tmp}` idiom before looking.
scan_abs_path_literals() {
  sed -e 's/^[[:space:]]*#.*$//' -e 's|${TMPDIR:-/tmp}||g' "$1" \
    | grep -nE '(^|[^A-Za-z0-9_./-])/(tmp|data|home|Users|var/folders)/'
}
# POSITIVE CONTROL FIRST. A scanner that matches nothing reports every file clean, and this
# one's whole job is to find a needle nobody has planted yet — so it must find a KNOWN one
# before its silence about the real files means anything (the standing rule: a sweep needs a
# positive control). The planted line is the shipped defect's own shape.
ctl_dir="$tmp/pathscan-control"; mkdir -p "$ctl_dir"
# THE PLANTED LINE IS ASSEMBLED, never written out: a control containing the literal it
# tests for would make this suite violate its own rule and red the very case below. Same
# split-the-needle idiom the roborev guard uses so it cannot match its own source.
ctl_root='/tmp'
{
  printf '%s\n' '#!/usr/bin/env bash'
  printf '# a comment mentioning %s/claude-1000/session/scratchpad/rec must NOT red\n' "$ctl_root"
  printf '%s\n' 'safe=$(mktemp -d "${TMPDIR:-/tmp}/ok.XXXXXX")'
  printf 'PATH="$d:%s/claude-1000/dead-session/scratchpad/rec:$PATH"\n' "$ctl_root"
} >"$ctl_dir/planted.sh"
ctl_hits=$(scan_abs_path_literals "$ctl_dir/planted.sh")
if printf '%s' "$ctl_hits" | grep -q 'dead-session' \
   && [ "$(printf '%s\n' "$ctl_hits" | grep -c .)" -eq 1 ]; then
  ok "abs-path guard: the scanner finds a planted session-scratchpad PATH entry (and only it)"
else
  bad "abs-path guard: the scanner did not isolate the planted violation: $ctl_hits"
fi
# ...and now the real files, which must be clean.
for pathscan_f in "$0" "$CAPLIB"; do
  pathscan_hits=$(scan_abs_path_literals "$pathscan_f")
  if [ -z "$pathscan_hits" ]; then
    ok "abs-path guard: ${pathscan_f##*/} names no host- or session-specific absolute path"
  else
    bad "abs-path guard: ${pathscan_f##*/} carries a host/session-specific absolute path: $pathscan_hits"
  fi
done

# =====================================================================================
# 23b. STRUCTURAL: NO LIVE COMMAND SUBSTITUTION INSIDE A DOUBLE-QUOTED `eval` STRING.
#      Backticks and `$(...)` are LIVE inside "..." — including inside a single-quoted
#      run nested in it, because the OUTER quotes are the double ones. The shipped defect:
#      `eval "$__od='no \`claude\` on PATH ...'"` executed `claude`, printing
#      `claude: command not found` into the bootstrap transcript at exactly the moment an
#      operator is debugging why the box cannot start claude — and deleting the subject
#      from the message ("no  on PATH"). It was harmless only because the branch is guarded
#      on the binary being ABSENT; that is luck, not design.
#      Asserted STRUCTURALLY rather than per message, because it is a CLASS: a behavioural
#      case only covers the two instances someone already found.
# WHOLE-LINE COMMENTS ARE EXCLUDED, and that exclusion is not a loophole: a backtick in a
# comment is inert. Caught by this guard's own first run, which red on a comment QUOTING the
# defect — a guard that reds on correct input is the guard agents learn to delete.
# DECLARED NON-EXHAUSTIVE: it reads ONE LINE AT A TIME, so an eval string continued across
# lines is not covered. Every eval in this file is single-line today.
evalsrc=$(grep -n 'eval "' "$CAPLIB" | grep -v '^[0-9]*:[[:space:]]*#')
evalbt=$(printf '%s\n' "$evalsrc" | grep -v '\\`' | grep '`' || true)
evalcs=$(printf '%s\n' "$evalsrc" | grep -E '(^|[^\\])\$\(' || true)
if [ -z "$evalbt" ] && [ -z "$evalcs" ]; then
  ok "no unescaped backtick or \$( ) survives inside a double-quoted eval string"
else
  bad "a live command substitution sits inside a double-quoted eval: $evalbt $evalcs"
fi

# =====================================================================================
# 23. NO RUN PRINTS A TOKEN-SHAPED VALUE. Asserted over the WHOLE suite transcript, not
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
# ALWAYS run — a floor set to the TOTAL would red whenever the one legitimately skippable
# case (the real-tmux isolation case, 3 assertions) skips, and a floor that reds on correct
# input is the floor agents learn to delete. The platform-guard case is NO LONGER skippable:
# a host without `uname` is a named refusal at startup, because that host would take the
# non-Linux branch in every case.
CASE_FLOOR=91
if [ "$((PASS + FAIL))" -lt "$CASE_FLOOR" ]; then
  printf 'FAIL - case floor: %s cases ran, expected at least %s (cases were lost)\n' "$((PASS + FAIL))" "$CASE_FLOOR"
  exit 1
fi
[ "$FAIL" -eq 0 ]
