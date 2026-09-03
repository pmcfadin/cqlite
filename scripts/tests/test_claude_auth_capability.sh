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
# Hence two independent observation lines, and hence this suite drives them independently.
#
# AND NEITHER LINE CERTIFIES ANYTHING (#3733, lead ruling). They were CERTIFYING verdicts
# whose passing state was `VERIFIED`; three consecutive independent reviews each found a
# NEW High of one shape — the probe cannot observe the property its verdict named — so the
# DESIGN changed instead of the code being carved a fourth time. Section 34 is where that
# lives, as an INVARIANT rather than case by case, because a rename can be undone one state
# at a time while an invariant reds on the first reintroduction.
#
# WHAT IT ASSERTS BEYOND "THE CODE IS THERE":
#   * every state is REACHABLE and correctly labeled, by planting its condition;
#   * an INHERITED CLAUDE_CODE_OAUTH_TOKEN can never produce a positive observation about a
#     PERSISTED one (#3414's shipped defect, one subject over) — the scrub case;
#   * rc alone and output alone are each INSUFFICIENT for PROBE-ANSWERED (both halves
#     required);
#   * NO input yields a state named VERIFIED, every report declares its own scope, and the
#     BEST- and WORST-looking inputs share an exit status — so nothing downstream can gate
#     on one (section 34);
#   * an alternate credential in the probe's environment is REPORTED, and the line does not
#     claim the persisted value authenticated (section 35);
#   * all five documented limitations are FINDABLE at their code sites (section 36);
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

# THE CONFIG-DIR FIXTURES ARE REAL PATHS, because a `*-BOTH` state requires the directory to
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
  # SUDO_USER/SUDO_UID ARE SCRUBBED BY DEFAULT. The tmux verdict now resolves the INVOKING
  # agent's identity from them, so a suite run under `sudo` would put every case on the
  # delegation path and measure something else entirely. Cases that want that posture set
  # them explicitly, which is also how they stay legible as being about it.
  out=$(PATH="$shimdir:$PATH" env -u SUDO_USER -u SUDO_UID \
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
#   slowstart   — no server, and a would-be server takes SEVERAL SECONDS to start the pane
#                 (still inside the probe bound, and the pane still reports). The only mode
#                 that puts an interrupt INSIDE the cold probe's own working-directory
#                 lifetime, which is what the interrupt-safety case below needs.
#   seedfail    — reads exactly like `stale`, but every `setenv` is RECORDED and then FAILS.
#                 The one mode in which an explicitly requested repair is REACHED and does
#                 not complete, which is the only way to observe what bootstrap does with
#                 the repair's exit status (#3733 F1).
#   wedged      — the server accepts the call and NEVER ANSWERS (show-environment sleeps
#                 far past any bound). The shape a half-dead tmux server has in the field.
#   substitute  — no server, and a would-be server SUBSTITUTES a DIFFERENT token of the
#                 SAME LENGTH into the pane (a `set-environment` in a tmux config does
#                 exactly this). The delivered value is wrong; every length-derived
#                 measurement of it is indistinguishable from the right one.
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
      slowstart)
        # A DEDICATED MARKER, written before the sleep: the interrupt case below must signal
        # while the probe is INSIDE its armed window, and `new-session` is the first thing
        # the probe does AFTER arming its traps. Polling for the working DIRECTORY instead
        # was measurably racy — `mktemp -d` runs BEFORE the arm, so the signal could land in
        # the unarmed gap and red correct code.
        printf 'entered\n' >"$d/slowstart-entered"
        sleep 3; sh -c "\$cmd"; exit \$? ;;
      substitute)
        # A server-side substitution, which is what a \`set-environment\` line in a tmux
        # config does. The pane receives a DIFFERENT value of the SAME LENGTH.
        env CLAUDE_CODE_OAUTH_TOKEN='$TOK_OTHER' sh -c "\$cmd"
        exit \$? ;;
    esac
    sh -c "\$cmd"
    exit \$? ;;
  kill-server)
    printf 'kill-server sock=%s\n' "\$sock" >>"\$log"
    exit 0 ;;
  show-environment)
    case '$mode' in
      no-server|probefail|substitute|slowstart) printf 'no server running on %s/tmux-1000/default\n' "\${TMPDIR:-/tmp}" >&2; exit 1 ;;
      broken)     printf 'lost server\n' >&2; exit 1 ;;
      wedged)     sleep 120; exit 0 ;;
      missing)    printf 'CLAUDE_CONFIG_DIR=%s\nPATH=/usr/bin\n' '$cfg'; exit 0 ;;
      stale|seedfail) printf 'CLAUDE_CODE_OAUTH_TOKEN=%s\nCLAUDE_CONFIG_DIR=%s\n' '$TOK_OTHER' '$cfg'; exit 0 ;;
      incomplete) printf 'CLAUDE_CODE_OAUTH_TOKEN=%s\n-CLAUDE_CONFIG_DIR\n' '$TOK'; exit 0 ;;
      complete)   printf 'CLAUDE_CODE_OAUTH_TOKEN=%s\nCLAUDE_CONFIG_DIR=%s\n' '$TOK' '$cfg'; exit 0 ;;
    esac ;;
  set-environment|setenv)
    case '$mode' in wedged) sleep 120; exit 0 ;; esac
    shift
    key=''; val=''
    while [ "\$#" -gt 0 ]; do
      case "\$1" in -*) ;; *) if [ -z "\$key" ]; then key="\$1"; else val="\$1"; fi ;; esac
      shift
    done
    printf 'setenv %s len=%s\n' "\$key" "\${#val}" >>"\$log"
    case '$mode' in seedfail) exit 1 ;; esac
    exit 0 ;;
esac
exit 0
EOF
  chmod +x "$d/tmux"
}
# plant_id <dir> <me> <euid> <known-user> <known-uid> [<alt-user> <alt-uid>]: an `id` that
# answers about FIXED identities, so a case can put the script in the posture `sudo` creates
# (running as root while the INVOKING agent is someone else) without needing root or a
# second real account. An unknown user is a FAILED lookup, exactly as the real `id` reports one.
#
# TWO PAIRS, NOT ONE, AND THAT IS WHAT MAKES THE INCONSISTENT-METADATA GUARD REACHABLE. With
# a single name<->uid pair every "SUDO_USER and SUDO_UID disagree" case necessarily supplied
# a uid this stub knows NOTHING about, so the script answered on the EARLIER branch (`SUDO_UID
# does not resolve to an account`) and the consistency guard was never entered — neutering it
# left the suite fully green. A second, INDEPENDENT pair lets a case name a uid that DOES
# resolve, to a DIFFERENT login than SUDO_USER, which is the only shape that reaches it. The
# alt arms are emitted only when supplied, so the existing single-pair call sites are unchanged.
plant_id() {
  local d="$1" me="$2" euid="$3" ku="$4" kuid="$5" ku2="${6:-}" kuid2="${7:-}" alt=''
  if [ -n "$ku2" ] && [ -n "$kuid2" ]; then
    alt=$(printf '  "-u %s")   printf %s %s ;;\n  "-un %s") printf %s %s ;;' \
      "$ku2" "'%s\\n'" "'$kuid2'" "$kuid2" "'%s\\n'" "'$ku2'")
  fi
  cat >"$d/id" <<EOF
#!/usr/bin/env bash
case "\$*" in
  '-un')  printf '%s\n' '$me' ;;
  '-u')   printf '%s\n' '$euid' ;;
  "-u $ku")   printf '%s\n' '$kuid' ;;
  "-un $kuid") printf '%s\n' '$ku' ;;
$alt
  -u*|-n*) printf 'id: no such user\n' >&2; exit 1 ;;
  *)      printf 'id: unsupported in this stub: %s\n' "\$*" >&2; exit 1 ;;
esac
EOF
  chmod +x "$d/id"
}
# plant_delegator <dir> <name>: a `runuser`/`sudo` stub that RECORDS the delegation and then
# runs the rest of the command line, so the case can assert BOTH that the call was
# delegated to the right login AND that the delegated command still reached tmux.
plant_delegator() {
  local d="$1" nm="$2"
  cat >"$d/$nm" <<EOF
#!/usr/bin/env bash
log="$d/delegate-calls.log"
user=''
while [ "\$#" -gt 0 ]; do
  case "\$1" in
    -u) user="\$2"; shift 2 ;;
    -n) shift ;;
    --) shift; break ;;
    *) break ;;
  esac
done
printf '%s user=%s cmd=%s\n' '$nm' "\$user" "\$*" >>"\$log"
exec "\$@"
EOF
  chmod +x "$d/$nm"
}
# plant_chown <dir> <rc>: a recording `chown` that succeeds or fails on demand. The cold
# probe hands its private working directory to the invoking user before a delegated tmux
# can write into it, and both outcomes are verdict-bearing.
#
# IT ALSO RECORDS WHAT EXISTED AT HANDOVER TIME, which is what makes the ORDERING testable
# at all (#3733 LIMITATION 4). The hazard is not a wrong verdict: root used to write
# `probe.sh` into a directory it had ALREADY given away, and on this fleet every lane runs
# as ONE user, so the recipient is a peer lane — which can plant a symlink at that path and
# have ROOT follow it. The race itself is not testable (the interleaving is not
# controllable), but the invariant "root creates everything it owns BEFORE transferring" is:
# at the moment `chown` runs, `probe.sh` must already be there. `-h`/`-R` and any flags are
# skipped so the recorded subject is the PATH, not an option.
plant_chown() {
  local d="$1" crc="$2"
  cat >"$d/chown" <<EOF
#!/usr/bin/env bash
printf 'chown %s\n' "\$*" >>"$d/chown-calls.log"
subj=''
for a in "\$@"; do case "\$a" in -*) ;; *) subj="\$a" ;; esac; done
if [ -n "\$subj" ] && [ -d "\$subj" ]; then
  if [ -e "\$subj/probe.sh" ]; then st=present; else st=absent; fi
  printf 'at-handover probe.sh=%s\n' "\$st" >>"$d/chown-calls.log"
fi
exit $crc
EOF
  chmod +x "$d/chown"
}

# plant_absent <dir> <tool>: make <tool> UNRESOLVABLE even though /usr/bin is on PATH.
# A stub that exits 127 is not the same fact as an absent binary, so this suite instead
# runs the capability script with a MINIMAL PATH holding only the shim dir plus coreutils.
# minpath: a PATH holding ONLY the coreutils the capability script needs — so `claude`
# and `tmux` are genuinely ABSENT rather than stubbed. A stub exiting 127 is not the same
# FACT as a missing binary, and this suite asserts the missing-binary verdict.
MINPATH="$tmp/minpath"; mkdir -p "$MINPATH"
for t in bash sh uname grep sed tail cut tr mktemp rm env cat head sort comm find chmod ln mkdir timeout true id; do
  # `type -P`, NOT `command -v`: for a name that is ALSO a shell builtin (`true`) the
  # latter answers with the bare word, and `ln -sf true "$MINPATH/true"` then plants a
  # SELF-REFERENTIAL symlink — measured, and it made `timeout … true` fail 126 ("too many
  # levels of symbolic links"), so the timeout resolver refused and every case on this PATH
  # reported the wrong cause. `type -P` searches PATH only. The absolute-path guard is belt.
  src=$(type -P "$t" 2>/dev/null) || continue
  case "$src" in /*) ;; *) continue ;; esac
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
# THE EXIT STATUS IS ABOUT THE REPORT, NOT ABOUT THE BOX (#3733, lead ruling). This line
# no longer certifies anything, so no exit status may encode a pass: a report that was
# PRINTED exits 0 whatever it found. Section 24 pins the invariant that the best- and
# worst-looking inputs are INDISTINGUISHABLE by exit status, which is what stops a caller
# gating on it.
if [ "$rc" -eq 0 ]; then
  ok "claude-auth: a printed report exits 0 — the status is about the report, not the box"
else
  bad "claude-auth: a printed NOT-PERSISTED report exited $rc — the status must not encode a verdict"
fi

# =====================================================================================
# 1b. THE SCRUB (issue #3414's shipped defect, one subject over). Bootstrap runs inside a
#     session that ALREADY carries the token, so an unscrubbed check answers about the
#     INHERITED value while claiming to answer about the PERSISTED one. With nothing
#     persisted and a perfectly good token in the environment, the verdict must still be
#     NOT-PERSISTED — never PROBE-ANSWERED.
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
# 2. PROBE-ANSWERED — persisted token, and the probe returns BOTH rc 0 AND the sentinel.
#    THE TOKEN IS DELIBERATELY NOT `VERIFIED` (#3733, lead ruling): what was observed is
#    that a `claude -p` run whose environment carried the persisted value answered, which
#    is NOT the same claim as "the persisted value is what authenticated" — see LIMITATION
#    2 in the library. The state name says what happened; it certifies nothing.
# =====================================================================================
d2=$(mkshim "$tmp/s2"); ef2="$tmp/env2"
mkenvfile "$ef2" "CLAUDE_CODE_OAUTH_TOKEN=$TOK" "CLAUDE_CONFIG_DIR=$CFGDIR"
plant_claude_probe_env "$d2"
run_cap "$d2" "$ef2" -- --auth
if printf '%s' "$out" | grep -q '^claude-auth: PROBE-ANSWERED'; then
  ok "claude-auth: PROBE-ANSWERED when a run carrying the persisted token returns the sentinel"
else
  bad "claude-auth: expected PROBE-ANSWERED, got: $out"
fi
# ...AND THE LINE DISCLAIMS THE CLAIM IT USED TO MAKE. The old wording said the persisted
# credential "authenticated"; nothing here can observe WHICH credential in the probe's
# environment did, so the detail must name that limitation rather than assert the stronger
# fact. A state rename with the old sentence behind it would be a cosmetic demotion.
# SCOPED TO THE OBSERVATION LINE, not the whole output: the scope note printed beside it
# also talks about limitations, so an unscoped grep would pass on the note alone and assert
# nothing about the line under test.
if printf '%s' "$out" | grep '^claude-auth:' | grep -q 'LIMITATION 2'; then
  ok "claude-auth: the answered-probe detail names the alternate-credential limitation"
else
  bad "claude-auth: the answered-probe detail claims more than was observed: $out"
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
if printf '%s' "$out" | grep -q '^claude-auth: PROBE-ANSWERED'; then
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
  ok "claude-auth: rc 0 with NO sentinel is not PROBE-ANSWERED (and is UNMEASURED, not an accusation)"
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
  ok "claude-auth: the sentinel with rc != 0 is not PROBE-ANSWERED (and names no rejection, so UNMEASURED)"
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
if printf '%s' "$out" | grep -q '^claude-auth: PROBE-ANSWERED'; then
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
  "complete:SERVER-CARRIES-BOTH:token matches and CLAUDE_CONFIG_DIR is present" \
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
# ...AND THE REFUSAL IS STILL A REPORT, so it exits 0 like every other one. Pinned because
# the usage text makes exactly this claim ("even a refused test-only seam prints a line and
# exits 0"), and a help string is a claim about behaviour that decays like a comment. It is
# also the nearest thing to a "could not produce a report" path, i.e. the one an exit-code
# gate would most plausibly be written against.
if [ "$rc" -eq 0 ]; then
  ok "the seam refusal is still a printed report, so it exits 0 like every other one"
else
  bad "the seam refusal exited $rc — the report modes must carry no verdict in their status"
fi
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
# EVERY DECIDING GREP OVER A BOOTSTRAP RUN READS A HERE-STRING, NEVER A PIPE, and this is
# not a style preference — it is a defect that was reproduced here. `set -o pipefail` is on
# and `grep -q` EXITS ON THE FIRST MATCH, so with a producer that still has more than a pipe
# buffer to write, `printf` takes SIGPIPE and dies 141 and the PIPELINE reports FAILURE for
# a SUCCESSFUL match. A full bootstrap run is exactly that much output: the overwrite-warning
# case below red on a run whose warning was plainly present in the log. The library was fixed
# for the same shape (its matcher block says so); the suite's own assertions over the SMALL
# `$out` payloads are unaffected and are left alone, which is also why this only bit here.
# The `bad` messages still pipe — those greps read to EOF and decide nothing.
# bs_marked <text> <marker-ere> <message-prefix-ere>: the lines of a bootstrap run that
# carry [<marker>] AND whose MESSAGE begins with <message-prefix>. rc 0 iff there is one.
#
# WHY A HELPER AND NOT A `grep -E '\[warn\].*claude-auth'`: that form was written, and it
# MATCHED A LINE ABOUT SOMETHING ELSE ENTIRELY —
# `[warn] no 'origin' remote in <tmpdir>/bs-root — cannot check push credentials` — because
# this suite's own `mktemp` prefix is `claude-auth-test`, so the tmpdir PATH in an unrelated
# warning contains the needle. A `.*` between a marker and a name is a whole-line search
# masquerading as a structural one, and the payload it searches contains attacker-… no, worse
# than attacker-controlled: OUR OWN PATHS. So the message prefix is ANCHORED to the position
# right after the marker.
# AND THE MARKER IS COLOUR-WRAPPED, so the escapes are stripped AT THE PARSE SITE — the same
# rule CLAUDE.md states for cargo output: `ok()`/`warn()` print `\033[32m[ok]\033[0m`, so a
# pattern spanning marker-then-space matches nothing against the raw text. ESC is produced by
# `printf` rather than written as `\x1b`, which is a GNU-sed extension.
bs_marked() {
  local __esc; __esc=$(printf '\033')
  # BOTH INTERPOLATIONS ARE GROUPED. Unparenthesised, `$2` = `ok|warn` splits the WHOLE
  # pattern at the `|` — it became `(^ *\[ok)|(warn\] +(…))`, whose left branch matched
  # `[ok]   cargo present`. An alternation dropped into a pattern without a group is the
  # second "structural match that is really a whole-line search" in this one helper.
  sed -e "s/${__esc}\[[0-9;]*m//g" <<<"$1" | grep -E "^[[:space:]]*\[($2)\][[:space:]]+($3)"
}
# THE HELPER'S OWN POSITIVE CONTROL, and it is not optional: every assertion below that
# uses `bs_marked` is a NEGATIVE one ("no marked claude line exists"), so a helper that
# matched NOTHING would make all of them pass vacuously — a searcher must be shown finding
# what it is looking for. Both of this helper's real defects are planted here (a
# colour-wrapped marked line it must FIND, and an unrelated warning whose PATH contains
# `claude-auth` which it must NOT), so a regression in either direction reds immediately
# instead of greening the negatives. Hermetic: a synthetic transcript, no bootstrap run.
# THREE PROBES, ONE PER DEFECT, each its OWN transcript so a clause cannot be satisfied by
# the wrong line: (A) a colour-wrapped `[warn] claude-auth:` must MATCH; (B) an unrelated
# warning whose PATH contains `claude-auth` must NOT (the tmpdir-name defect); (C) an
# `[ok]` line about something else must NOT match under the `ok|warn` alternation (the
# ungrouped-alternation defect, whose left branch matched `[ok]   cargo present`).
bs_probe_a=$(printf '  \033[33m[warn]\033[0m claude-auth: OPT-OUT (planted)\n')
bs_probe_b=$(printf '  \033[33m[warn]\033[0m no origin remote in /t/claude-auth-test.XX/bs-root\n')
bs_probe_c=$(printf '  \033[32m[ok]\033[0m   cargo present\n')
if bs_marked "$bs_probe_a" warn 'claude-auth:' >/dev/null \
   && ! bs_marked "$bs_probe_b" warn 'claude-auth' >/dev/null \
   && ! bs_marked "$bs_probe_c" 'ok|warn' 'claude-auth:|claude-tmux-env:' >/dev/null; then
  ok "marker scanner: finds a colour-wrapped marked line; a tmpdir PATH holding the needle and an unrelated [ok] are not ones"
else
  bad "marker scanner: bs_marked mis-classifies one of its three planted probes (A match / B path-only / C other-[ok])"
fi
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
if grep -q 'claude-auth: OPT-OUT' <<<"$bs_out"; then
  ok "bootstrap: --skip-claude-auth emits a LOUD claude-auth: OPT-OUT verdict"
else
  bad "bootstrap: --skip-claude-auth produced no OPT-OUT line"
fi
# THE OPT-OUT NO LONGER WITHHOLDS, AND THAT IS THE POINT (#3733, lead ruling). It used to
# be a `[warn]`, i.e. `--strict` failed on it. But the thing being declined is a REPORT that
# certifies nothing, so failing `--strict` for declining it made `--strict` pass or fail on
# a line that is no longer a verdict — which is precisely what the ruling forbids. The
# opt-out stays LOUD (asserted above) and is now non-verdict-bearing, like the section it
# skips. Note this run also passes --skip-push-probe, whose OPT-OUT is a REAL [warn] and
# does withhold, so the assertion is scoped to the claude-auth line's own posture rather
# than to the summary string.
if bs_marked "$bs_out" warn 'claude-auth: OPT-OUT' >/dev/null; then
  bad "bootstrap: the claude-auth opt-out is still a [warn], so --strict fails on a non-verdict: $(printf '%s' "$bs_out" | grep 'claude-auth: OPT-OUT')"
else
  ok "bootstrap: the claude-auth opt-out is loud but carries NO [warn] — --strict cannot fail on it"
fi
bs_out2=$(run_bootstrap --skip-smoke --skip-push-probe)
printf '%s\n' "$bs_out2" >>"$TRANSCRIPT"
if grep -q 'claude-auth: PROBE-ANSWERED' <<<"$bs_out2" \
   && grep -q 'claude-tmux-env: SERVER-CARRIES-BOTH' <<<"$bs_out2"; then
  ok "bootstrap: both observation lines surface through bootstrap's reporter"
else
  bad "bootstrap: the observation lines are missing from the run"
  printf '%s\n' "$bs_out2" | sed -n '/Claude credential/,/^$/p'
fi
# NEITHER LINE MAY BE AN [ok] OR A [warn], ON THE BEST-LOOKING BOX THERE IS (#3733). This
# is the bootstrap half of section 34's invariant and it needs its own case, because the
# library can be perfectly honest while its primary consumer re-certifies its output:
# `ok` is what `--strict` reads, and `warn` is what makes `--strict` fail — so a line
# carrying either marker is a verdict again whatever the library called it. This fixture is
# the ANSWERED/CARRIES-BOTH box, i.e. the only input that could ever have earned an [ok].
if bs_marked "$bs_out2" 'ok|warn' 'claude-auth:|claude-tmux-env:' >/dev/null; then
  bad "bootstrap: a claude credential line is still an [ok]/[warn] verdict: $(bs_marked "$bs_out2" 'ok|warn' 'claude-auth:|claude-tmux-env:')"
else
  ok "bootstrap: neither claude credential line is an [ok] or a [warn] — --strict cannot read them"
fi
# ...and the scope note reaches bootstrap's output too. Bootstrap is the primary consumer
# and its output is what an operator pastes, so a scope note printed only by the standalone
# CLI would be absent exactly where it is read.
if grep -q 'claude-auth-report: OBSERVATIONS-ONLY' <<<"$bs_out2"; then
  ok "bootstrap: the OBSERVATIONS-ONLY scope note reaches bootstrap's own output"
else
  bad "bootstrap: bootstrap printed the two lines with no scope note beside them"
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
if [ "$usage_rc" = 2 ] && grep -q 'contradictory' <<<"$usage_out"; then
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
#     picker. SERVER-CARRIES-BOTH now requires an AFFIRMATIVE match against the persisted value AND
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
# token). UNMEASURED, never a delivery state.
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
#     emit a DELIVERY state and recommend Linux-only /etc/environment remedies. When this
#     case was written a false one was an [ok], and [ok] is what `--strict` reads (#3414: scoping a
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
if printf '%s' "$out" | grep -q '^claude-tmux-env: COLD-START-DELIVERS-BOTH'; then
  ok "claude-tmux-env: COLD-START-DELIVERS-BOTH names what a throwaway server handed its pane"
else
  bad "claude-tmux-env: a fresh box with a good persisted environment was not reported: $out"
fi
# THE COLD STATE IS TEXTUALLY DISTINCT FROM THE LIVE ONE (#3733, lead ruling), and that is
# the point: the cold probe INJECTS the /etc/environment values into its own throwaway
# server, so it observes tmux PROPAGATION and NOT pam_env DELIVERY (LIMITATION 1). One
# token for both would have hidden which of the two was actually seen.
if printf '%s' "$out" | grep '^claude-tmux-env:' | grep -q 'LIMITATION 1 of 5'; then
  ok "claude-tmux-env: the cold-start line names the propagation-not-PAM-delivery limitation"
else
  bad "claude-tmux-env: the cold-start line claims more than the probe observed: $out"
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
if [ "$rc" -eq 0 ]; then
  ok "claude-tmux-env: COLD-START-MISSING is REPORTED, and the exit status stays 0"
else
  bad "claude-tmux-env: COLD-START-MISSING exited $rc — the status must not encode a verdict"
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
if [ "$rc" -eq 0 ]; then
  ok "claude-tmux-env: NO-SERVER is REPORTED, and the exit status stays 0"
else
  bad "claude-tmux-env: NO-SERVER exited $rc — the status must not encode a verdict"
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
  if printf '%s' "$out" | grep -q '^claude-tmux-env: COLD-START-DELIVERS-BOTH'; then
    ok "cold-start probe: REAL tmux, no live server, good persisted environment -> COLD-START-DELIVERS-BOTH"
  else
    bad "cold-start probe: the real-tmux cold path did not report a delivery: $out"
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
if printf '%s' "$out" | grep -q '^claude-auth: PROBE-ANSWERED'; then
  ok "claude-auth: a sentinel on line 1 of 200 KB of output is still seen (no SIGPIPE inversion)"
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
if printf '%s' "$out" | grep -q '^claude-tmux-env: SERVER-CARRIES-BOTH' && [ "$many_elapsed" -lt 30 ]; then
  ok "claude-tmux-env: a 5000-variable server environment is read correctly and FINISHES (${many_elapsed}s)"
else
  bad "claude-tmux-env: a 5000-variable server environment was misread or did not finish (${many_elapsed}s): $out"
fi
d25b=$(mkshim "$tmp/s25b"); plant_tmux_bulky "$d25b"
run_cap "$d25b" "$ef2" -- --tmux-env
if printf '%s' "$out" | grep -q '^claude-tmux-env: SERVER-CARRIES-BOTH'; then
  ok "claude-tmux-env: a correctly seeded server with 200 KB of environment is still read correctly"
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
  ok "claude-auth: the probe is NOT RUN when the bound cannot be enforced (a would-be PROBE-ANSWERED stub was never invoked)"
else
  bad "claude-auth: the unbounded probe RAN anyway — a missing capability inherited the permissive branch"
fi
if printf '%s' "$out" | grep -qi 'kill\|hard'; then
  ok "claude-auth: the refusal NAMES the missing hard bound rather than reporting a generic absence"
else
  bad "claude-auth: the refusal does not say which capability was missing: $out"
fi
# The tmux path shares the resolver, so it refuses on the same ground — and since the LIVE
# read is bounded too (section 32) the refusal now happens one step EARLIER, before that
# read. That is the more honest verdict, not merely a different one: with no enforceable
# bound we cannot take the live read at all, so we do not know whether a server is running,
# and `NO-SERVER` would assert something unmeasured. UNMEASURED, non-passing, cause named.
d28c=$(mkshim "$tmp/s28c"); plant_tmux "$d28c" no-server
for tname in timeout gtimeout; do cp "$d28b/$tname" "$d28c/$tname"; done
run_cap "$d28c" "$ef2" -- --tmux-env
if printf '%s' "$out" | grep -q '^claude-tmux-env: UNMEASURED'; then
  ok "claude-tmux-env: an unenforceable bound refuses the tmux path outright (UNMEASURED)"
else
  bad "claude-tmux-env: the tmux path ran without an enforceable bound: $out"
fi
# THE NAMING IS THE WHOLE ASSERTION NOW: the exit status carries no verdict (section 34),
# so requiring a non-zero one here would be requiring the certification this issue removed.
if printf '%s' "$out" | grep '^claude-tmux-env:' | grep -qi 'kill\|hard\|UNBOUNDED'; then
  ok "claude-tmux-env: ...and it names the missing hard bound rather than a generic absence"
else
  bad "claude-tmux-env: the tmux-path refusal does not name the bound: $out"
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
# THE TIE RULE, WHICH IS THE WHOLE ORDERING. A message naming BOTH a non-credential
# service failure and a credential rejection is AMBIGUOUS, and an accusation about a
# credential must be EARNED: its remedy is "replace the VALUE". The matchers were ordered
# transport -> rejection -> service, so a mixed response was classified FAILED and bootstrap
# told the operator to discard a potentially valid token — exactly the harm the FAILED/
# UNMEASURED split exists to remove, surviving in the tie case. Two shapes, because a tie
# arises from either service matcher half.
claude_auth_case 'a 429 whose body ALSO names an authentication error' s29j 1 \
  'API Error: 429 {"type":"error","error":{"type":"rate_limit_error","message":"Number of requests has exceeded your rate limit"}} (upstream reported authentication_error while retrying)' UNMEASURED
claude_auth_case 'a 503 gateway page that ALSO says unauthorized' s29k 1 \
  'API Error: 503 Service Unavailable — the gateway returned: unauthorized' UNMEASURED
# ...and the ordering must not have swallowed the rejection verdict: a message naming ONLY
# a rejection still earns FAILED (asserted by s29f/s29g/s29h above), and one naming only a
# service failure is still UNMEASURED (s29a-s29d).
# A 401 must not be matched inside a longer number — the guard against a matcher that
# earns an accusation from a coincidence.
claude_auth_case 'a 401 embedded in a larger number is NOT a rejection' s29i 1 \
  'API Error: 1401337 requests queued upstream' UNMEASURED

# =====================================================================================
# 30. `--yes` MUST NOT SEED AT ALL, AND `--fix-claude-auth` IS THE OPERATOR'S DECISION.
#     THE HAZARD IS UNCHANGED AND IS WORTH RESTATING, because the fix for it changed shape.
#     On a box whose PERSISTED token is bad while the RUNNING server holds a working one,
#     seeding overwrites the working value and every lane spawned afterwards fails to
#     authenticate — the repair BREAKING a working box, unattended, since
#     `.agent-ami/profile.yaml` runs bootstrap this way. SERVER-STALE is precisely where it
#     fires: "the server's token DIFFERS from the persisted one" reads the same whether the
#     server's copy is the stale one or the only good one on the machine, and nothing in
#     that line can tell them apart.
#     THE OLD FIX WAS TO REQUIRE `claude-auth: VERIFIED` FIRST, AND THAT IS NOW WITHDRAWN
#     (#3733, lead ruling): the gate's whole purpose was to stop a bad token overwriting a
#     working server, and `VERIFIED` was a PROXY that three reviews showed can be true on a
#     box whose persisted credential was never the thing that authenticated (LIMITATION 2).
#     A gate on a proxy that can be false-positive in exactly the direction that causes the
#     harm is worse than no gate, because it invites the unattended seeding it cannot
#     justify. So the unattended path no longer seeds AT ALL, and seeding is what an
#     operator gets by typing `--fix-claude-auth` and reading the warning it prints.
#     WHAT THIS COSTS, stated rather than discovered later: `--yes` no longer repairs a
#     SERVER-MISSING/STALE box in passing. That is the intended trade — the unattended run
#     reports, and a human decides.
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
if grep -q 'claude-auth: FAILED' <<<"$bs30a"; then
  ok "seed gate: the harm case really does present a REJECTED persisted credential"
else
  bad "seed gate: the fixture did not produce claude-auth: FAILED, so the case tests nothing: $(printf '%s' "$bs30a" | grep -c .) lines"
fi
if [ ! -f "$d30a/tmux-calls.log" ] || ! grep -q '^setenv ' "$d30a/tmux-calls.log"; then
  ok "seed gate: --yes performs NO seed — the unattended path never mutates the server"
else
  bad "seed gate: --yes seeded the running server unattended: $(cat "$d30a/tmux-calls.log")"
fi
# A REPAIR THAT QUIETLY DOES NOT HAPPEN READS AS "NOTHING WAS WRONG", so `--yes` must SAY
# that seeding is now the operator's call and NAME the command. This is the case that stops
# the change being "silently stopped repairing".
# `-e`, because the pattern STARTS WITH `--` and grep would otherwise read it as options.
if grep -q -e '--fix-claude-auth' <<<"$bs30a"; then
  ok "seed gate: the unattended run NAMES the operator-driven repair instead of doing it"
else
  bad "seed gate: --yes declined to seed SILENTLY: $(printf '%s' "$bs30a" | sed -n '/Claude credential/,/^$/p' | head -12)"
fi
if grep -q 'claude-tmux-env: SERVER-STALE' <<<"$bs30a"; then
  ok "seed gate: the tmux observation is REPORTED UNCHANGED (the run neither repaired nor hid it)"
else
  bad "seed gate: the tmux observation was not left as found: $(printf '%s' "$bs30a" | grep 'claude-tmux-env:')"
fi
# (b) THE EXPLICIT FLAG STILL SEEDS, ON THE SAME REJECTED-CREDENTIAL FIXTURE. Two things at
#     once, and both matter. Without this the change could have been "never seed", which
#     passes (a) and helps nobody. And running it on the fixture the OLD design REFUSED is
#     what proves the proxy gate is gone rather than merely relocated: the decision moved
#     to the human who typed the flag, and the run must say what it is about to overwrite.
#     Re-measured rather than asserted — `tmux setenv` exiting 0 is a claim about the
#     command, not about the server's environment.
d30b=$(mkshim "$tmp/s30b"); plant_bootstrap_quiet_stubs "$d30b"
plant_claude "$d30b" 1 'Failed to authenticate: OAuth session expired and could not be refreshed'
plant_tmux_stateful "$d30b" "$TOK_OTHER" "$CFGDIR"
bs30b=$(run_bootstrap_in "$d30b" "$ef2" --skip-smoke --skip-push-probe --fix-claude-auth)
printf '%s\n' "$bs30b" >>"$TRANSCRIPT"
if grep -q 'claude-auth: FAILED' <<<"$bs30b"; then
  ok "seed gate: the explicit-flag case presents the SAME rejected credential the old gate refused"
else
  bad "seed gate: the explicit-flag fixture is not the refused one, so it proves nothing: $(printf '%s' "$bs30b" | grep 'claude-auth:')"
fi
if [ -f "$d30b/tmux-calls.log" ] && grep -q '^setenv CLAUDE_CODE_OAUTH_TOKEN len=' "$d30b/tmux-calls.log"; then
  ok "seed gate: an explicit --fix-claude-auth seeds, with no proxy gate in front of it"
else
  bad "seed gate: --fix-claude-auth did not seed — the gate was relocated, not removed: $(cat "$d30b/tmux-calls.log" 2>/dev/null)"
fi
# THE OVERWRITE WARNING MUST BE THERE **AND** MUST NOT BE A `[warn]`. Both halves: a
# destructive action needs a loud statement, and `--strict --fix-claude-auth` must not exit
# 1 on a repair the operator explicitly asked for and which SUCCEEDED — that would be a
# verdict about an action, the same confusion this issue removed one subject over.
if grep -qi 'OVERWRIT' <<<"$bs30b" \
   && ! bs_marked "$bs30b" warn 'claude-auth' >/dev/null; then
  ok "seed gate: the explicit repair STATES the overwrite, and carries no [warn]"
else
  bad "seed gate: --fix-claude-auth seeded an unvalidated value with no warning: $(printf '%s' "$bs30b" | sed -n '/Claude credential/,/^$/p' | head -14)"
fi
if grep -q 'claude-tmux-env: SERVER-CARRIES-BOTH' <<<"$bs30b"; then
  ok "seed gate: the repair is RE-REPORTED against the changed server"
else
  bad "seed gate: the seeded server was not re-read: $(printf '%s' "$bs30b" | grep 'claude-tmux-env:')"
fi

# =====================================================================================
# 31. THE COLD-START PROBE MUST COMPARE THE VALUE, NOT ITS LENGTH.
#     The probe reports what a would-be server DELIVERS to a pane, and the delivered token
#     was checked only against `${#persisted}`. A tmux configuration that substitutes a
#     DIFFERENT value of the same length — one `set-environment` line, and every fleet box
#     has a tmux config — therefore produced `VERIFIED`, which was then the verdict that
#     certified a fresh box as able to start a lane. Length equality is not value equality.
#     THE SUBSTITUTED FIXTURE IS ASSERTED TO BE THE SAME LENGTH FIRST, because the case is
#     only evidence about VALUE comparison if a LENGTH comparison would have passed it: a
#     different-length fixture would red against the defect too, for the wrong reason.
#     The comparison itself is by SALTED DIGEST and neither value is printed — see the
#     no-leak assert over the whole transcript at the end of this file.
# =====================================================================================
if [ "${#TOK}" -eq "${#TOK_OTHER}" ]; then
  ok "cold-start probe: the substitution fixture is the SAME LENGTH as the persisted token"
else
  bad "cold-start probe: the substitution fixture differs in LENGTH (${#TOK} vs ${#TOK_OTHER}) — the case below would pass against a length comparison and proves nothing"
fi
d31=$(mkshim "$tmp/s31"); plant_tmux "$d31" substitute
run_cap "$d31" "$ef2" -- --tmux-env
if printf '%s' "$out" | grep -q -e '^claude-tmux-env: COLD-START-DELIVERS-BOTH' -e '^claude-tmux-env: SERVER-CARRIES-BOTH'; then
  bad "cold-start probe: a same-length SUBSTITUTED token was reported as a delivery: $out"
elif printf '%s' "$out" | grep -q '^claude-tmux-env: NO-SERVER'; then
  ok "cold-start probe: a same-length substituted token is NOT verified (UNMEASURED-class)"
else
  bad "cold-start probe: unexpected verdict for a substituted token: $out"
fi
if [ "$rc" -eq 0 ]; then
  ok "cold-start probe: the substituted-token report exits 0 like every other report"
else
  bad "cold-start probe: a substituted token exited $rc — the status must not encode a verdict"
fi
if printf '%s' "$out" | grep -qi 'does not match the persisted'; then
  ok "cold-start probe: the detail says the DELIVERED value does not match the persisted one"
else
  bad "cold-start probe: the detail does not name a value mismatch: $out"
fi
# ...and the identity comparison must not have broken the honest path: the unmodified
# cold-start case (section 21) still VERIFIES, which is asserted there.
# ...and the identity comparison is a PRECONDITION, not a nicety: with no digest tool on
# PATH the only comparison left is by length, so the probe REFUSES rather than falling back
# to it. MINPATH deliberately links no sha256sum/shasum. A missing capability must not
# inherit the permissive branch — the same rule the timeout resolver already obeys.
d31b=$(mkshim "$tmp/s31b"); plant_tmux "$d31b" no-server
rc=0
out=$(PATH="$d31b:$MINPATH" env -u SUDO_USER -u SUDO_UID CQLITE_BOOTSTRAP_TEST_MODE=1 \
      CQLITE_CLAUDE_AUTH_ENV_FILE="$ef2" bash "$CAPLIB" --tmux-env 2>&1) || rc=$?
printf '%s\n' "$out" >>"$TRANSCRIPT"
if printf '%s' "$out" | grep -q '^claude-tmux-env: NO-SERVER' \
   && printf '%s' "$out" | grep -qi 'digest'; then
  ok "cold-start probe: no digest tool on PATH is a NAMED refusal, never a length fallback"
else
  bad "cold-start probe: a missing digest tool did not refuse: $out"
fi


# =====================================================================================
# 32. BOUNDING IS A PROPERTY OF EVERY EXTERNAL CALL IN THIS FILE, NOT OF ONE CALL SITE.
#     This is the SECOND bounding finding here — round 2's was the `claude -p` probe's
#     SIGTERM-only bound — so it is closed as a class rather than carved a third time. A
#     tmux server that ACCEPTS a connection and never answers hangs `show-environment -g`
#     (and `setenv -g`) forever, and bootstrap is a provisioning entry point that
#     `.agent-ami/profile.yaml` runs unattended: an unbounded read there is an indefinite
#     hang, not a slow check. A timeout is classified UNMEASURED on a READ and a failed
#     repair on a WRITE — never a pass, and never an accusation about the credential.
#
#     THREE ASSERTS, because none of them alone closes the class:
#      (a) the helper really escalates on a real child (a unit-level bound);
#      (b) a wedged LIVE server yields UNMEASURED, in bounded wall-clock, end to end;
#      (c) STRUCTURAL: no `tmux` invocation in the library is unbounded — which is what
#          stops the next call site being added without one, and it carries its own
#          positive control because a scanner that matches nothing reports every file clean.
# =====================================================================================
# (a) the helper itself. Sourced rather than driven through the CLI, because the property
# is about the helper and a CLI path would also measure everything around it.
bnd_t0=$SECONDS
bnd_rc=0
( set -uo pipefail; . "$CAPLIB"; claude_auth_bounded 1 sleep 30 ) >/dev/null 2>&1 || bnd_rc=$?
bnd_dt=$((SECONDS - bnd_t0))
if [ "$bnd_rc" = 124 ] || [ "$bnd_rc" = 137 ]; then
  ok "bounded runner: a long child is killed and reports a timeout rc ($bnd_rc)"
else
  bad "bounded runner: a 30s child under a 1s bound returned rc $bnd_rc"
fi
# The threshold is generous on purpose (bound 1s + a 5s SIGKILL escalation + process
# startup, on a box that may be running a gate): what it has to distinguish is "the bound
# fired" from "we waited out the child's own 30s lifetime", and 20 does that.
if [ "$bnd_dt" -le 20 ]; then
  ok "bounded runner: ...and it returned in ${bnd_dt}s, not the child's own lifetime"
else
  bad "bounded runner: the bound took ${bnd_dt}s to fire — it did not bound anything"
fi

# (b) a WEDGED live server, end to end. The verdict must be UNMEASURED (a read that could
# not be taken is not a verdict about the box) and it must arrive without waiting for the
# stub's own 120s sleep.
d32=$(mkshim "$tmp/s32"); plant_tmux "$d32" wedged
w_t0=$SECONDS
run_cap "$d32" "$ef2" -- --tmux-env
w_dt=$((SECONDS - w_t0))
if printf '%s' "$out" | grep -q '^claude-tmux-env: UNMEASURED'; then
  ok "claude-tmux-env: a tmux server that never answers is UNMEASURED, not a verdict"
else
  bad "claude-tmux-env: a wedged server did not report UNMEASURED: $out"
fi
if [ "$w_dt" -le 60 ]; then
  ok "claude-tmux-env: ...and the bound fired in ${w_dt}s instead of hanging the entry point"
else
  bad "claude-tmux-env: the wedged-server read took ${w_dt}s — it is not bounded"
fi
if printf '%s' "$out" | grep -qi 'did not answer\|bound'; then
  ok "claude-tmux-env: the detail names the bound rather than inventing a cause"
else
  bad "claude-tmux-env: the wedged-server detail does not name the bound: $out"
fi

# (d) AN INTERRUPT DURING THE BOUNDED READ MUST LEAVE NOTHING BEHIND. Bounding the live
# read turned the window between the stderr file's `mktemp` and its `rm` from microseconds
# into up to the op bound against a wedged server, and that file sits in a directory we do
# not own. Found by this suite's own interrupted run leaving one — a widened leak window is
# a cost of the fix, not a pre-existing condition, so it is pinned here.
d32b=$(mkshim "$tmp/s32b"); plant_tmux "$d32b" wedged
int_before=$(find "${TMPDIR:-/tmp}" -maxdepth 1 -name 'cqlite-tmuxenv.*' 2>/dev/null | sort)
PATH="$d32b:$PATH" env -u SUDO_USER -u SUDO_UID CQLITE_BOOTSTRAP_TEST_MODE=1 \
  CQLITE_CLAUDE_AUTH_ENV_FILE="$ef2" bash "$CAPLIB" --tmux-env >/dev/null 2>&1 &
int_pid=$!
# The read is bounded at ten seconds and the stub sleeps two minutes, so two seconds in it
# is certainly inside the window this case is about.
sleep 2
kill -TERM "$int_pid" 2>/dev/null
wait "$int_pid" 2>/dev/null
int_after=$(find "${TMPDIR:-/tmp}" -maxdepth 1 -name 'cqlite-tmuxenv.*' 2>/dev/null | sort)
if [ "$int_before" = "$int_after" ]; then
  ok "interrupt safety: SIGTERM during the bounded live read leaves no stderr temp file"
else
  bad "interrupt safety: an interrupted bounded read leaked a temp file: $(comm -13 <(printf '%s\n' "$int_before") <(printf '%s\n' "$int_after"))"
fi

# ...AND THE SAME MUST HOLD FOR EACH PROBE'S WORKING DIRECTORY, WHICH NOTHING ASSERTED.
# `claude_auth_probe_arm_traps` has THREE call sites — the credential probe's throwaway
# CLAUDE_CONFIG_DIR, the live read's stderr file (above), and the cold probe's private
# working directory — and only the stderr file was pinned, so DELETING either directory's arm
# call left this suite fully green while an interrupt leaked a directory into a tree we do not
# own. A leak per SIGINT on a provisioning entry point is exactly the shape the trap exists
# for, and an unfalsifiable guard is worse than none.
#
# EACH CASE RUNS UNDER ITS OWN PRIVATE TMPDIR, so the assertion is EXACT (the private tree
# holds no leftovers) rather than a before/after diff of a /tmp shared with concurrent lanes.
#
# glob_matches <path-glob>: print each EXISTING entry matching <path-glob>, one per line.
# A GLOB, not `find`, and the SAME helper for both halves of each case (the wait and the
# assertion): a `find` reduced to an emptiness test collapses "the scan failed" onto "no
# match" — the shape this repo lints as `1699-find-tristate` — and here that collapse would
# read a failed scan as a clean tree, which is the permissive direction.
glob_matches() {
  local c
  for c in $1; do [ -e "$c" ] && printf '%s\n' "$c"; done
  return 0
}
# wait_for_glob <path-glob>: bounded poll until <path-glob> matches; rc 1 if it never did.
# THE WINDOW IS MEASURED, NOT ASSUMED — signalling BEFORE the artifact exists would pass
# vacuously, with nothing to leak, so a window never entered is a FAILURE below, not a pass.
wait_for_glob() {
  local i=0
  while [ "$i" -lt 200 ]; do
    [ -n "$(glob_matches "$1")" ] && return 0
    sleep 0.05; i=$((i + 1))
  done
  return 1
}

# (d1) THE CREDENTIAL PROBE's throwaway CLAUDE_CONFIG_DIR (armed between its `mktemp -d` and
# the up-to-90s network call, so the window is the widest of the three).
#
# THE POLL WAITS ON A MARKER THE PROBE WRITES *AFTER* ARMING, NOT ON THE DIRECTORY. Waiting
# on the directory was measurably racy and red CORRECT code: `mktemp -d` runs BEFORE the arm,
# so a signal fired the instant the directory appeared could land in the unarmed gap, where
# bash's default SIGTERM disposition kills the process and the leak is real but is NOT the
# defect this case is about. The stub is the first thing to run INSIDE the armed window.
int_a_tmp="$tmp/int-auth-tmp"; mkdir -p "$int_a_tmp"
int_a_mark="$tmp/int-auth-reached"; rm -f "$int_a_mark"
d32c=$(mkshim "$tmp/s32c")
# Slow enough that the signal lands while the throwaway config dir exists, far short of the
# 90s probe bound. Like every other claude stub here it never echoes its argv or environment:
# the value under test is a secret.
cat >"$d32c/claude" <<EOF
#!/usr/bin/env bash
printf 'entered\n' >'$int_a_mark'
sleep 4
printf 'the interrupt should have arrived long before this\n'
exit 1
EOF
chmod +x "$d32c/claude"
PATH="$d32c:$PATH" env -u SUDO_USER -u SUDO_UID CQLITE_BOOTSTRAP_TEST_MODE=1 \
  TMPDIR="$int_a_tmp" CQLITE_CLAUDE_AUTH_ENV_FILE="$ef2" bash "$CAPLIB" --auth >/dev/null 2>&1 &
int_a_pid=$!
if wait_for_glob "$int_a_mark"; then
  kill -TERM "$int_a_pid" 2>/dev/null
  wait "$int_a_pid" 2>/dev/null
  int_a_left=$(glob_matches "$int_a_tmp/cqlite-claude-probe.*")
  if [ -z "$int_a_left" ]; then
    ok "interrupt safety: SIGTERM during the credential probe leaves no throwaway config dir"
  else
    bad "interrupt safety: an interrupted credential probe leaked its config dir: $int_a_left"
  fi
else
  kill -TERM "$int_a_pid" 2>/dev/null; wait "$int_a_pid" 2>/dev/null
  bad "interrupt safety: the credential probe never reached its bounded network call, so the armed window this case is about was never entered"
fi

# (d2) THE COLD-START PROBE's private working directory (it also carries the tmux socket, so
# a leak here strands a throwaway server's socket too). `slowstart` is the mode that holds the
# probe inside that window: no live server, so the cold path is taken, and the would-be
# server takes several seconds to start the pane.
# Same marker discipline as (d1): `slowstart` records that `new-session` was ENTERED, which
# happens only after the arm, so the signal cannot land in the pre-arm gap.
int_b_tmp="$tmp/int-cold-tmp"; mkdir -p "$int_b_tmp"
d32d=$(mkshim "$tmp/s32d"); plant_tmux "$d32d" slowstart
PATH="$d32d:$PATH" env -u SUDO_USER -u SUDO_UID CQLITE_BOOTSTRAP_TEST_MODE=1 \
  TMPDIR="$int_b_tmp" CQLITE_CLAUDE_AUTH_ENV_FILE="$ef2" bash "$CAPLIB" --tmux-env >/dev/null 2>&1 &
int_b_pid=$!
if wait_for_glob "$d32d/slowstart-entered"; then
  kill -TERM "$int_b_pid" 2>/dev/null
  wait "$int_b_pid" 2>/dev/null
  int_b_left=$(glob_matches "$int_b_tmp/cqlite-tmux-probe.*")
  if [ -z "$int_b_left" ]; then
    ok "interrupt safety: SIGTERM during the cold-start probe leaves no private working dir"
  else
    bad "interrupt safety: an interrupted cold-start probe leaked its working dir: $int_b_left"
  fi
else
  kill -TERM "$int_b_pid" 2>/dev/null; wait "$int_b_pid" 2>/dev/null
  bad "interrupt safety: the cold-start probe never started its throwaway server, so the armed window this case is about was never entered"
fi

# (c) STRUCTURAL. Logical lines (backslash continuations joined) so a wrapped invocation is
# judged whole; whole-line comments blanked; `command -v tmux` is a resolution, not a call;
# and a line whose `tmux` sits inside a MESSAGE (`printf`) is not an invocation. DECLARED
# NON-EXHAUSTIVE: it is a textual scan, so a tmux call assembled at run time from a
# variable is not covered, and neither is one on a line whose quoting it mis-pairs. Quoted
# spans are removed before matching, so a tmux named in a MESSAGE is not an invocation — a
# guard that reds on the sentence describing its own subject is the guard agents learn to
# delete. The positive control below pins what it DOES catch.
scan_unbounded_tmux() {
  # ESCAPED QUOTES GO FIRST, then quoted spans. Rendering a detail through
  # eval with an escaped inner quote is this file's normal idiom, and without dropping
  # those backslash-quote pairs the span matcher pairs the WRONG quotes and leaves the
  # message text exposed as if it were code.
  # QUOTED SPANS ARE REMOVED SECOND, and that is what makes this decidable rather than a
  # list of exclusions: an invocation is a bare command WORD, while every mention of tmux in
  # a MESSAGE lives inside quotes. Excluding `printf`/`eval` lines by name (the first
  # version) both missed message text in a plain assignment and would have hidden a real
  # call sharing a line with a printf.
  sed -e ':a' -e '/\\$/{N;s/\\\n//;ta' -e '}' \
      -e 's/^[[:space:]]*#.*$//' -e 's/\\"//g' -e 's/"[^"]*"//g' -e "s/'[^']*'//g" "$1" \
    | grep -vE 'command -v tmux' \
    | grep -nE '(^|[^[:alnum:]_.-])tmux[[:space:]]' \
    | grep -vE 'claude_auth_bounded|claude_auth_tmux_run|# bounded-exempt:'
}
ctl3="$tmp/unbounded-tmux-control.sh"
{
  printf '%s\n' '#!/usr/bin/env bash'
  printf '%s\n' '# a comment naming tmux kill-server must not red'
  printf '%s\n' 'claude_auth_bounded 10 tmux show-environment -g'
  printf '%s\n' 'tmux kill-server >/dev/null 2>&1'
} >"$ctl3"
ctl3_hits=$(scan_unbounded_tmux "$ctl3")
if printf '%s' "$ctl3_hits" | grep -q 'kill-server' \
   && [ "$(printf '%s\n' "$ctl3_hits" | grep -c .)" -eq 1 ]; then
  ok "unbounded-tmux guard: the scanner finds a planted unbounded invocation (and only it)"
else
  bad "unbounded-tmux guard: the scanner did not isolate the planted call: [$ctl3_hits]"
fi
unb_hits=$(scan_unbounded_tmux "$CAPLIB")
if [ -z "$unb_hits" ]; then
  ok "unbounded-tmux guard: every tmux invocation in the library runs under a hard bound"
else
  bad "unbounded-tmux guard: an unbounded tmux invocation survives: $unb_hits"
fi

# =====================================================================================
# 33. UNDER THE DOCUMENTED `sudo` INVOCATION, THE LIVE TMUX OPERATIONS MUST TARGET THE
#     INVOKING AGENT'S SERVER — NOT ROOT'S.
#     Bootstrap prints, and the fleet runbook documents, `sudo bash
#     scripts/bootstrap-agent-machine.sh --yes`. A tmux client with no `-S`/`-L` talks to
#     the CURRENT UID's default server, so under sudo both the inspection and the repair
#     addressed ROOT's server while the agent's own — the one that actually spawns lanes —
#     stayed broken. Root usually has no server at all, so the read fell through to the
#     cold-start probe, which measures the PERSISTED FILE and passes: a false cold-start
#     VERIFIED on a box that still cannot start a lane. That is the precise failure this
#     whole issue exists to eliminate, reintroduced one layer down.
#     The identity is RESOLVED, not guessed: where it cannot be resolved the verdict is
#     UNMEASURED and the repair REFUSES — never a silent fall back to the current UID,
#     which is the permissive branch wearing a default's clothes.
# =====================================================================================
INVOKER='cqlite-lane-invoker'
# A SECOND, INDEPENDENT ACCOUNT. Only the conflicting-metadata case needs it: "SUDO_USER and
# SUDO_UID disagree" is a statement about TWO resolvable identities, so a uid that resolves to
# nobody expresses a DIFFERENT refusal (the case below it) and cannot reach the guard.
OTHER_LOGIN='cqlite-other-login'
OTHER_UID=1234
# (a) DELEGATED READ. The posture `sudo` creates: running as root, SUDO_USER naming the
# agent. The tmux call must be delegated to that login AND must still deliver its reading —
# a delegation that lost the answer would be a different defect with the same log line.
d33=$(mkshim "$tmp/s33"); plant_tmux "$d33" stale; plant_id "$d33" root 0 "$INVOKER" 4711
plant_delegator "$d33" runuser
run_cap "$d33" "$ef2" "SUDO_USER=$INVOKER" "SUDO_UID=4711" -- --tmux-env
if [ -f "$d33/delegate-calls.log" ] && grep -q "user=$INVOKER" "$d33/delegate-calls.log" \
   && grep -q 'cmd=.*tmux' "$d33/delegate-calls.log"; then
  ok "sudo posture: the live tmux read is delegated to the INVOKING agent, not run as root"
else
  bad "sudo posture: no delegated tmux read was recorded: $(cat "$d33/delegate-calls.log" 2>/dev/null)"
fi
if printf '%s' "$out" | grep -q '^claude-tmux-env: SERVER-STALE'; then
  ok "sudo posture: ...and the delegated read still produces the invoking user's verdict"
else
  bad "sudo posture: the delegated read did not deliver a reading: $out"
fi

# (b) AN UNRESOLVABLE IDENTITY IS UNMEASURED, NEVER THE CURRENT UID. The tmux stub here is
# `complete`, i.e. root's server is perfect — so a silent fall back to the current UID
# would report a DELIVERY about the wrong server, which is exactly the false certification.
d33b=$(mkshim "$tmp/s33b"); plant_tmux "$d33b" complete; plant_id "$d33b" root 0 "$INVOKER" 4711
plant_delegator "$d33b" runuser
run_cap "$d33b" "$ef2" 'SUDO_USER=cqlite-no-such-login-3733' 'SUDO_UID=6553' -- --tmux-env
if printf '%s' "$out" | grep -q '^claude-tmux-env: SERVER-CARRIES-BOTH'; then
  bad "sudo posture: an unresolvable invoking identity reported the CURRENT UID's server: $out"
elif printf '%s' "$out" | grep -q '^claude-tmux-env: UNMEASURED'; then
  ok "sudo posture: an unresolvable invoking identity is UNMEASURED, never a fall back"
else
  bad "sudo posture: unexpected verdict for an unresolvable identity: $out"
fi
if [ ! -f "$d33b/tmux-calls.log" ] || ! grep -q 'show-environment' "$d33b/tmux-calls.log"; then
  ok "sudo posture: ...and no tmux server was consulted at all"
else
  bad "sudo posture: a tmux server was consulted despite an ambiguous target: $(cat "$d33b/tmux-calls.log")"
fi
# A CONFLICTING identity is ambiguous too: SUDO_UID that does not match the uid SUDO_USER
# resolves to means the two halves of the record disagree, and one of them is wrong.
#
# THE CAUSE TEXT IS ASSERTED, NOT JUST THE VERDICT TOKEN, and BOTH accounts resolve. Every
# ambiguity refusal in this block emits the SAME `UNMEASURED` token, so a verdict-only grep
# is satisfied by whichever guard happens to fire first — which is how this case previously
# passed while measuring the `SUDO_UID does not resolve to an account` branch instead, with
# the consistency guard unentered and unfalsifiable. The uid here therefore resolves (to
# OTHER_LOGIN) and SUDO_USER resolves to a DIFFERENT uid, which is the only shape that
# reaches the guard, and the detail is what proves which one answered.
d33c=$(mkshim "$tmp/s33c"); plant_tmux "$d33c" complete
plant_id "$d33c" root 0 "$INVOKER" 4711 "$OTHER_LOGIN" "$OTHER_UID"
plant_delegator "$d33c" runuser
run_cap "$d33c" "$ef2" "SUDO_USER=$INVOKER" "SUDO_UID=$OTHER_UID" -- --tmux-env
if printf '%s' "$out" | grep -q '^claude-tmux-env: UNMEASURED' \
   && printf '%s' "$out" | grep -q 'INCONSISTENT sudo metadata'; then
  ok "sudo posture: SUDO_USER and SUDO_UID disagreeing is named INCONSISTENT, not a guess"
else
  bad "sudo posture: a conflicting sudo identity did not report UNMEASURED with the INCONSISTENT-metadata cause: $out"
fi

# SUDO_UID IS THE AUTHORITY AND SUDO_USER MUST AGREE — the rule bootstrap's gate-pin
# retarget already follows (#3414 roborev round 8), reused rather than re-derived. Sudo sets
# BOTH, so exactly one of them present is incomplete metadata (stale, hand-exported,
# inherited), and incomplete metadata about WHICH SERVER TO ANSWER ABOUT is ambiguity.
d33c2=$(mkshim "$tmp/s33c2"); plant_tmux "$d33c2" complete; plant_id "$d33c2" root 0 "$INVOKER" 4711
plant_delegator "$d33c2" runuser
# The cause text is asserted here for the same reason: a MISSING SUDO_UID also matches the
# NEXT guard's `not numeric` test (the empty string is not numeric), so on the verdict token
# alone this case passed with the incomplete-record guard neutered entirely.
run_cap "$d33c2" "$ef2" "SUDO_USER=$INVOKER" -- --tmux-env
if printf '%s' "$out" | grep -q '^claude-tmux-env: UNMEASURED' \
   && printf '%s' "$out" | grep -q 'the sudo record is incomplete'; then
  ok "sudo posture: a SUDO_USER with no SUDO_UID is named incomplete metadata, not a hint"
else
  bad "sudo posture: incomplete sudo metadata was not reported as such: $out"
fi
# ...and a uid that resolves to nobody is its own refusal, distinct from a name that does not
# match it: they are different operator facts, so each asserts its OWN wording — which is
# also what keeps the two cases from silently collapsing onto one branch again.
d33c3=$(mkshim "$tmp/s33c3"); plant_tmux "$d33c3" complete; plant_id "$d33c3" root 0 "$INVOKER" 4711
plant_delegator "$d33c3" runuser
run_cap "$d33c3" "$ef2" "SUDO_USER=$INVOKER" 'SUDO_UID=6553' -- --tmux-env
if printf '%s' "$out" | grep -q '^claude-tmux-env: UNMEASURED' \
   && printf '%s' "$out" | grep -q 'does not resolve to an account'; then
  ok "sudo posture: a SUDO_UID that resolves to no account is UNMEASURED, and says so"
else
  bad "sudo posture: an unresolvable SUDO_UID was not reported as unresolvable: $out"
fi

# (c) THE REPAIR OBEYS THE SAME RULE. Seeding root's server while the agent's stays broken
# is worse than not seeding: it reports success and changes nothing that matters.
d33d=$(mkshim "$tmp/s33d"); plant_tmux "$d33d" missing; plant_id "$d33d" root 0 "$INVOKER" 4711
plant_delegator "$d33d" runuser
run_cap "$d33d" "$ef2" 'SUDO_USER=cqlite-no-such-login-3733' 'SUDO_UID=6553' -- --fix-tmux-env
if [ ! -f "$d33d/tmux-calls.log" ] || ! grep -q '^setenv' "$d33d/tmux-calls.log"; then
  ok "sudo posture: the repair REFUSES to seed when the target server is ambiguous"
else
  bad "sudo posture: the repair seeded an ambiguous server: $(cat "$d33d/tmux-calls.log")"
fi
if printf '%s' "$out" | grep -qi 'REFUSED'; then
  ok "sudo posture: ...and it says so rather than reporting a silent success"
else
  bad "sudo posture: the ambiguous repair did not name its refusal: $out"
fi
d33e=$(mkshim "$tmp/s33e"); plant_tmux "$d33e" missing; plant_id "$d33e" root 0 "$INVOKER" 4711
plant_delegator "$d33e" runuser
run_cap "$d33e" "$ef2" "SUDO_USER=$INVOKER" "SUDO_UID=4711" -- --fix-tmux-env
if [ -f "$d33e/delegate-calls.log" ] && grep -q "user=$INVOKER cmd=.*setenv" "$d33e/delegate-calls.log"; then
  ok "sudo posture: the repair seeds the INVOKING agent's server"
else
  bad "sudo posture: the repair did not delegate its seed: $(cat "$d33e/delegate-calls.log" 2>/dev/null)"
fi

# (d) NO SUDO, NO DELEGATION. The ordinary invocation must be unchanged — a wrapper applied
# when nobody asked for one is its own way of talking to the wrong server.
d33f=$(mkshim "$tmp/s33f"); plant_tmux "$d33f" complete; plant_id "$d33f" root 0 "$INVOKER" 4711
plant_delegator "$d33f" runuser
run_cap "$d33f" "$ef2" -- --tmux-env
if [ ! -f "$d33f/delegate-calls.log" ]; then
  ok "sudo posture: with no SUDO_USER the tmux call is NOT wrapped in a delegation"
else
  bad "sudo posture: a delegation was applied with no sudo in play: $(cat "$d33f/delegate-calls.log")"
fi
if printf '%s' "$out" | grep -q '^claude-tmux-env: SERVER-CARRIES-BOTH'; then
  ok "sudo posture: ...and the ordinary path still reaches its verdict"
else
  bad "sudo posture: the ordinary (non-sudo) path regressed: $out"
fi

# (e) THE COLD-START PROBE IS DELEGATED TOO, because a per-user tmux config is exactly what
# finding 1 showed can substitute the credential: a probe run as root measures ROOT's
# would-be server and says nothing about the agent's. Its private working directory must be
# handed over first, and a handover that FAILS is a refusal — never a quiet root-run probe.
d33g=$(mkshim "$tmp/s33g"); plant_tmux "$d33g" no-server; plant_id "$d33g" root 0 "$INVOKER" 4711
plant_delegator "$d33g" runuser; plant_chown "$d33g" 0
run_cap "$d33g" "$ef2" "SUDO_USER=$INVOKER" "SUDO_UID=4711" -- --tmux-env
if [ -f "$d33g/delegate-calls.log" ] && grep -q "user=$INVOKER cmd=.*new-session" "$d33g/delegate-calls.log"; then
  ok "sudo posture: the cold-start probe's server is started AS THE INVOKING AGENT"
else
  bad "sudo posture: the cold-start probe ran as the current UID: $(cat "$d33g/delegate-calls.log" 2>/dev/null)"
fi
if printf '%s' "$out" | grep -q '^claude-tmux-env: COLD-START-DELIVERS-BOTH'; then
  ok "sudo posture: ...and the delegated cold-start probe still reports a delivery"
else
  bad "sudo posture: the delegated cold-start probe did not verify: $out"
fi
d33h=$(mkshim "$tmp/s33h"); plant_tmux "$d33h" no-server; plant_id "$d33h" root 0 "$INVOKER" 4711
plant_delegator "$d33h" runuser; plant_chown "$d33h" 1
run_cap "$d33h" "$ef2" "SUDO_USER=$INVOKER" "SUDO_UID=4711" -- --tmux-env
if printf '%s' "$out" | grep -q '^claude-tmux-env: NO-SERVER' && printf '%s' "$out" | grep -qi 'could not be handed'; then
  ok "sudo posture: a failed handover of the probe directory REFUSES (UNMEASURED-class)"
else
  bad "sudo posture: a failed ownership handover did not refuse: $out"
fi

# =====================================================================================
# 36. THE FIVE DOCUMENTED LIMITATIONS MUST BE FINDABLE AT THEIR CODE SITES (#3733).
#     Under the lead's ruling these are limitations of a REPORT, not defects — but "not a
#     defect" is only true while a reader can FIND them. A limitation that lives in a
#     commit message, or in an issue thread, or in one paragraph of a header, is a
#     limitation the next person rediscovers as a bug and re-fixes; this file's own history
#     is three rounds of exactly that.
#     STRUCTURAL, AND DELIBERATELY MODEST. It asserts two things and NEITHER is that the
#     text is TRUE: that each of the five is marked `LIMITATION <n> of 5` as a COMMENT
#     somewhere in the library (so `grep` finds it at the code it belongs to, not only in
#     the header), and that the header INDEX names all five (so a reader who starts at the
#     top learns the set exists and is closed). Truth is not mechanically checkable here —
#     the same boundary #1716's tools-crate guard draws, recorded rather than papered over.
#     `of 5` is part of the marker on purpose: it makes the set CLOSED, so adding a sixth
#     limitation without renumbering reds this case instead of hiding in the tail.
# =====================================================================================
lim_missing=''
lim_unindexed=''
# THE HEADER IS CAPTURED ONCE, AND IT IS READ WITHOUT A PIPE. `awk … | grep -q` is the
# SIGPIPE-under-pipefail trap this repository documents and that the library itself was
# fixed for: `grep -q` exits on the first match, `awk` then dies 141, and `set -o pipefail`
# reports the PIPELINE as failed — for a SUCCESSFUL match. Measured while writing this case,
# and it was INTERMITTENT (limitations 1 and 5 red, 2-4 green, because whether awk had
# finished writing depended on where the match fell), which is the worst possible shape for
# a guard. A here-string has no second process to lose a race with.
lim_hdr=$(awk 'NR>1 && !/^#/ { exit } { print }' "$CAPLIB")
for lim_n in 1 2 3 4 5; do
  # A COMMENT, not merely the string: the runtime verdict DETAILS also cite these markers
  # (that is how an operator gets from a pasted line to the explanation), so a substring
  # match anywhere would be satisfied by the detail text alone and would assert nothing
  # about the code site. `#` before the marker on the same line is the discriminator.
  grep -qE '^[[:space:]]*#.*LIMITATION '"$lim_n"' of 5' "$CAPLIB" || lim_missing="${lim_missing:+$lim_missing }$lim_n"
  # The header index: a numbered entry in the leading comment block, which is bounded by
  # the first non-comment line so a match further down the file cannot stand in for it.
  grep -qE '^#[[:space:]]*'"$lim_n"'\.' <<<"$lim_hdr" || lim_unindexed="${lim_unindexed:+$lim_unindexed }$lim_n"
done
if [ -z "$lim_missing" ]; then
  ok "limitations: all five slots are marked as comments at their own code sites in the library"
else
  bad "limitations: no code-site comment marks LIMITATION(s) $lim_missing of 5 — a reader cannot find them"
fi
# THE LIVE/FIXED SPLIT IS ASSERTED, NOT JUST THE PRESENCE OF FIVE SLOTS (#3733). Slot 4 was
# RECLASSIFIED and FIXED — root wrote into a directory it had already handed over, which is a
# same-uid peer's symlink opportunity and so a defect rather than a limitation of a report —
# and its slot is KEPT as a record instead of renumbered, so references written while it was
# live still resolve. Without this, "five slots exist" would read identically whether slot 4
# is a live limitation or a fixed one, and a future silent claim that one of the OTHER four
# is fixed would pass too. Section 37 is what pins the fix itself.
lim_fixed_ok=0; lim_live_bad=''
for lim_n in 1 2 3 4 5; do
  if grep -qE '^[[:space:]]*#.*LIMITATION '"$lim_n"' of 5[^A-Za-z0-9]*\(#3733\)[^A-Za-z0-9]*(—|-)[[:space:]]*FIXED' "$CAPLIB"; then
    [ "$lim_n" = 4 ] && lim_fixed_ok=1 || lim_live_bad="${lim_live_bad:+$lim_live_bad }$lim_n"
  else
    [ "$lim_n" = 4 ] && lim_live_bad="${lim_live_bad:+$lim_live_bad }4(not-marked-FIXED)"
  fi
done
if [ "$lim_fixed_ok" = 1 ] && [ -z "$lim_live_bad" ]; then
  ok "limitations: slot 4 is recorded as FIXED and the other four are still live"
else
  bad "limitations: the live/FIXED split does not hold (slot-4-FIXED=$lim_fixed_ok, wrong: ${lim_live_bad:-none}) — a fixed slot and a live one must not read alike"
fi
if [ -z "$lim_unindexed" ]; then
  ok "limitations: the library header indexes all five, so the set reads as closed"
else
  bad "limitations: the header index omits LIMITATION(s) $lim_unindexed of 5"
fi
# THE POSITIVE CONTROL, because a scanner built to find something must be shown finding it
# (and, here, shown NOT finding what it must not): a sixth marker must red the closed-set
# assertion rather than pass unnoticed. Run against a COPY — this suite never mutates the
# library it is testing.
lim_copy="$tmp/caplib-sixth.sh"
# PREPENDED, not `sed`-substituted: a multi-line replacement needs a backslash-escaped
# newline in the script and the first draft of this line was an unterminated `s' command,
# so the control silently measured nothing. `cat` cannot get that wrong.
{ printf '# LIMITATION 6 of 5: a planted marker\n'; cat "$CAPLIB"; } >"$lim_copy"
if grep -qE '^[[:space:]]*#.*LIMITATION 6 of 5' "$lim_copy" \
   && ! grep -qE '^[[:space:]]*#.*LIMITATION 6 of 5' "$CAPLIB"; then
  ok "limitations: the scanner finds a planted sixth marker (and the real library has none)"
else
  bad "limitations: the scanner cannot distinguish a planted sixth marker from the real library"
fi

# =====================================================================================
# 38. AN EXPLICITLY REQUESTED REPAIR THAT FAILED MUST AFFECT THE EXIT STATUS (#3733 F1).
#     THE FAMILY: `claude_auth_fix_tmux_env | while read; do info; done` DISCARDS the
#     repair's own status. bootstrap runs `set -uo pipefail`, so the PIPELINE's status is
#     actually correct — and nothing reads it, and there is no `set -e`. So a failed or
#     refused repair was followed by re-reporting and the run could still exit 0, `--strict`
#     included. Same shape as `$?` after a pipe into `head`/`tail` and a trailing `echo`
#     masking a gate's status: a status that is right and unread.
#     WHY THIS IS NOT THE VERDICT THE #3733 RULING REMOVED. That ruling forbids a claim that
#     THE CREDENTIAL IS VALID, because no observation here can establish it. This is a
#     different subject: whether an ACTION THE OPERATOR EXPLICITLY REQUESTED completed, which
#     is directly observable from the action's own outcome. An action's success is a
#     legitimate verdict; a credential's validity is not. Do not "fix" this back.
#     TWO CASES, AND THE SECOND IS WHY THE FIRST CANNOT BE "ALWAYS warn". The existing
#     comment deliberately used `info` so `--strict --fix-claude-auth` does NOT red on a
#     SUCCESSFUL repair the operator asked for. That reasoning is correct and is pinned here,
#     so a future round cannot satisfy (a) by warning unconditionally.
# =====================================================================================
# (a) THE REPAIR FAILS. `seedfail` reads as SERVER-STALE — so the repair branch is REACHED —
#     and then refuses every setenv.
d38a=$(mkshim "$tmp/s38a"); plant_bootstrap_quiet_stubs "$d38a"
plant_claude_probe_env "$d38a"; plant_tmux "$d38a" seedfail
bs38a=$(run_bootstrap_in "$d38a" "$ef2" --skip-smoke --skip-push-probe --fix-claude-auth)
printf '%s\n' "$bs38a" >>"$TRANSCRIPT"
# THE FIXTURE MUST HAVE REACHED AND FAILED THE REPAIR, asserted first: if the branch was
# never entered, or the seed silently succeeded, the assertion below has no subject.
if grep -q 'fix FAILED' <<<"$bs38a" && [ -f "$d38a/tmux-calls.log" ] \
   && grep -q '^setenv CLAUDE_CODE_OAUTH_TOKEN len=' "$d38a/tmux-calls.log"; then
  ok "repair status: the fixture really did reach the repair and fail it (setenv attempted, fix FAILED reported)"
else
  bad "repair status: the fixture did not reach a FAILED repair, so the case below tests nothing: $(printf '%s' "$bs38a" | grep -i 'claude-auth' | head -4)"
fi
if bs_marked "$bs38a" warn 'claude-auth' >/dev/null; then
  ok "repair status: a FAILED explicit repair emits a [warn], so --strict reds on it"
else
  bad "repair status: a FAILED explicit repair emitted NO [warn] — its exit status is discarded and bootstrap can still exit 0: $(printf '%s' "$bs38a" | sed -n '/Claude credential/,/^$/p' | head -14)"
fi

# (b) THE REPAIR SUCCEEDS — and must NOT red. This is the distinction the F1 fix must not
#     collapse: a verdict about a health finding vs a verdict about a requested action.
d38b=$(mkshim "$tmp/s38b"); plant_bootstrap_quiet_stubs "$d38b"
plant_claude_probe_env "$d38b"; plant_tmux_stateful "$d38b" "$TOK_OTHER" "$CFGDIR"
bs38b=$(run_bootstrap_in "$d38b" "$ef2" --skip-smoke --skip-push-probe --fix-claude-auth)
printf '%s\n' "$bs38b" >>"$TRANSCRIPT"
if grep -q 'claude-tmux-env: SERVER-CARRIES-BOTH' <<<"$bs38b"; then
  ok "repair status: the success fixture really did complete its repair (re-read reports a delivery)"
else
  bad "repair status: the success fixture did not complete its repair, so the no-red assertion below is vacuous: $(printf '%s' "$bs38b" | grep 'claude-tmux-env:')"
fi
if bs_marked "$bs38b" warn 'claude-auth' >/dev/null; then
  bad "repair status: a SUCCESSFUL repair emitted a [warn] — --strict --fix-claude-auth would red on a repair that worked: $(bs_marked "$bs38b" warn 'claude-auth')"
else
  ok "repair status: a SUCCESSFUL repair emits NO [warn], so --strict --fix-claude-auth does not red on it"
fi
# AND THE DIFFERENCE IS EXACTLY ONE WARNING, which is what makes "--strict reds" a
# measurement rather than an inference: `warn()` increments the counter `--strict` reads, and
# these two runs differ in nothing but whether the seed succeeded. Counting the DELTA is
# immune to the sandbox's ambient warnings (an origin-less bs-root, the push-probe opt-out),
# which a bare `--strict` exit code is not — both runs would exit 1 on those alone.
w38a=$(bs_marked "$bs38a" 'ok|warn' '.' 2>/dev/null | grep -c '\[warn\]')
w38b=$(bs_marked "$bs38b" 'ok|warn' '.' 2>/dev/null | grep -c '\[warn\]')
if [ "$w38a" -eq "$((w38b + 1))" ]; then
  ok "repair status: the failing run carries EXACTLY ONE more [warn] than the succeeding one ($w38a vs $w38b)"
else
  bad "repair status: the failing/succeeding warning counts differ by $((w38a - w38b)), not 1 (failing=$w38a succeeding=$w38b) — the red is not attributable to the repair"
fi

# =====================================================================================
# 37. ROOT MUST CREATE EVERYTHING IT OWNS **BEFORE** IT TRANSFERS THE DIRECTORY (#3733,
#     was LIMITATION 4 of 5, now FIXED).
#     WHY THIS ONE IS NOT COVERED BY THE "IT IS ONLY A REPORT" RULING. The other four
#     limitations are PROXY defects: the observation claims more than it saw, and a wrong
#     claim damages nothing. This one is not a claim at all. Root wrote `probe.sh` into a
#     directory it had ALREADY chowned to the invoking user, and on this fleet EVERY LANE
#     RUNS AS ONE USER — so the recipient is a PEER LANE, which can plant a symlink at that
#     path and make ROOT truncate and overwrite an arbitrary file. That hazard exists
#     whenever the probe runs, whatever the line says, and it is a NON-INVOKER route: the
#     defect class this repo treats as real rather than out of model.
#     TWO ASSERTIONS, ONE BEHAVIOURAL AND ONE STRUCTURAL, AND THEY ARE LABELLED AS SUCH.
#     The RACE is not testable — the interleaving is not controllable — so nothing here
#     pretends to reproduce it. (a) BEHAVIOURAL: the recording `chown` stub reports what
#     existed in the directory at the moment it ran; `probe.sh` must already be there.
#     (b) STRUCTURAL: no write in the cold probe may target a path under the private
#     directory AFTER the `chown` line, which is the general invariant (a) can only sample —
#     (a) sees today's one write, (b) reds on the next one someone adds below the handover.
# =====================================================================================
# (a) BEHAVIOURAL. Same posture as section 33(e): running as root, SUDO_USER naming the
#     agent, a delegated cold-start probe, a recording chown.
d37=$(mkshim "$tmp/s37"); plant_tmux "$d37" no-server; plant_id "$d37" root 0 "$INVOKER" 4711
plant_delegator "$d37" runuser; plant_chown "$d37" 0
run_cap "$d37" "$ef2" "SUDO_USER=$INVOKER" "SUDO_UID=4711" -- --tmux-env
# THE FIXTURE MUST HAVE REACHED THE CHOWN AT ALL, asserted first: with no chown recorded,
# the ordering assertion below has no subject and would pass by vacuity.
if [ -f "$d37/chown-calls.log" ] && grep -q '^chown ' "$d37/chown-calls.log"; then
  ok "handover ordering: the delegated cold probe really did hand its directory over (chown recorded)"
else
  bad "handover ordering: no chown was recorded, so the ordering assertion below has no subject: $(cat "$d37/chown-calls.log" 2>/dev/null)"
fi
if grep -q '^at-handover probe.sh=present$' "$d37/chown-calls.log" 2>/dev/null; then
  ok "handover ordering: probe.sh already existed when the directory was handed over (root wrote it while it still owned the dir)"
else
  bad "handover ordering: root wrote probe.sh AFTER giving the directory away — a peer lane can symlink that path: $(cat "$d37/chown-calls.log" 2>/dev/null)"
fi

# (b) STRUCTURAL — labelled structural because it reads SOURCE, not behaviour. It cannot
#     observe a race and does not claim to; what it pins is that no write reachable in the
#     cold probe targets the handed-over directory after the handover.
#     THE ALIAS SET IS DERIVED, NEVER CURATED: `$__res` and `$__sock` are assigned FROM
#     `$__dir`, so a scan looking only for the literal `$__dir` would miss `: >"$__res"`
#     entirely — and a hand-listed set is a list to keep complete. Every local assigned a
#     value mentioning `$__dir` joins the set, so a new derived path is covered on the day
#     it is written.
#     THE BODY ENDS AT A BRACE THAT IS THE WHOLE LINE, and that is not pedantry: the pane
#     heredoc this function now writes FIRST contains `} >"$1"` at column zero, so a
#     `/^\}/` terminator truncated the body BEFORE the chown and the derivation failed —
#     which the fail-closed branch below correctly reported rather than passing. A scan that
#     cannot find its subject must FAIL, never green.
cp_body=$(awk '/^claude_tmux_cold_probe_into\(\) \{/ { on = 1 } on { print } on && /^\}$/ { exit }' "$CAPLIB")
cp_aliases=$(printf '%s\n' "$cp_body" \
  | sed -n 's/^[[:space:]]*\(local[[:space:]]\+\)\{0,1\}\(__[a-z0-9_]*\)=.*\$__dir.*/\2/p' | sort -u)
cp_chown_ln=$(printf '%s\n' "$cp_body" | grep -n '^[[:space:]]*if ! chown ' | head -1 | cut -d: -f1)
# cp_writes_after <body> <chown-line> <alias-list>: the line NUMBERS of redirections into a
# $__dir-derived path that sit at or below <chown-line>. Empty = invariant holds.
cp_writes_after() {
  local body="$1" ln="$2" aliases="$3" a='' hits=''
  for a in __dir $aliases; do
    hits="$hits$(printf '%s\n' "$body" | grep -nE '>[[:space:]]*"\$'"$a"'[/"]' \
                 | awk -F: -v L="$ln" '$1 >= L { print $0 }')"
  done
  printf '%s' "$hits"
}
if [ -z "$cp_chown_ln" ] || [ -z "$cp_aliases" ]; then
  bad "handover ordering (structural): the scan could not locate the chown line or derive the alias set from $CAPLIB — a failed derivation is a FAIL, never a pass"
else
  ok "handover ordering (structural): the scan located the chown and derived $(printf '%s\n' "$cp_aliases" | grep -c .) \$__dir-derived path name(s) from source"
  cp_bad=$(cp_writes_after "$cp_body" "$cp_chown_ln" "$cp_aliases")
  if [ -z "$cp_bad" ]; then
    ok "handover ordering (structural): no write targets the private directory at or after the handover"
  else
    bad "handover ordering (structural): a root write to the handed-over directory sits at or below the chown: $cp_bad"
  fi
  # THE POSITIVE CONTROL. Every assertion above is NEGATIVE, so a scanner that matched
  # nothing would green them all. Plant the ORIGINAL ordering — a write moved below the
  # chown — in a synthetic body and require the scan to find it.
  cp_planted=$(printf '%s\n' "$cp_body" | sed 's|^\([[:space:]]*\)cat >"$__dir/probe.sh".*|\1: PLACEHOLDER|')
  cp_planted=$(printf '%s\ncat >"$__dir/probe.sh" <<X\nX\n' "$cp_planted")
  cp_planted_ln=$(printf '%s\n' "$cp_planted" | grep -n '^[[:space:]]*if ! chown ' | head -1 | cut -d: -f1)
  if [ -n "$cp_planted_ln" ] && [ -n "$(cp_writes_after "$cp_planted" "$cp_planted_ln" "$cp_aliases")" ]; then
    ok "handover ordering (structural): the scan FINDS a write planted below the chown (so the clean verdict above is not vacuous)"
  else
    bad "handover ordering (structural): the scan cannot see a write planted below the chown — it proves nothing about the real body"
  fi
fi

# =====================================================================================
# 34. NEITHER LINE MAY EVER CERTIFY THIS BOX — THE INVARIANT, NOT AN INSTANCE (#3733,
#     lead ruling). Three consecutive independent reviews each found a NEW High of one
#     shape: the probe cannot observe the property its verdict named. The response was a
#     DESIGN change rather than a fourth fix — both lines are OBSERVATIONS now — and the
#     durable pin is this invariant, because a case-by-case rename can be undone one state
#     at a time while an invariant reds on the first reintroduction.
#
#     THREE PROPERTIES, and the third is the one that stops a caller acting on a proxy:
#      (a) no state either line emits is the token `VERIFIED` — the word a pasted log reads
#          as a certification, whatever the surrounding prose says;
#      (b) every entry point prints its own scope note, so an operator reading ONE pasted
#          line learns it certifies nothing;
#      (c) the BEST-looking and the WORST-looking inputs are INDISTINGUISHABLE by exit
#          status. That is what makes `if script --auth; then …` impossible to write, which
#          is the actual downstream hazard: a token rename leaves an exit-code gate intact.
#     The fixtures are the extremes of the whole state space: a perfect box (persisted
#     token, answering probe, server carrying both) and a bare one (nothing persisted, no
#     server, no claude).
# =====================================================================================
d34_best=$(mkshim "$tmp/s34best"); plant_claude_probe_env "$d34_best"; plant_tmux "$d34_best" complete
run_cap "$d34_best" "$ef2" -- --report
best_out="$out"; best_rc="$rc"
d34_worst=$(mkshim "$tmp/s34worst"); plant_tmux "$d34_worst" no-server
run_cap "$d34_worst" "$ef" -- --report
worst_out="$out"; worst_rc="$rc"
# THE FIXTURES MUST ACTUALLY BE THE TWO EXTREMES, asserted first: two inputs that landed on
# the SAME state would make (c) below pass without comparing anything (the "uniform output
# kills its own test" shape).
if printf '%s' "$best_out" | grep -q '^claude-auth: PROBE-ANSWERED' \
   && printf '%s' "$best_out" | grep -q '^claude-tmux-env: SERVER-CARRIES-BOTH' \
   && printf '%s' "$worst_out" | grep -q '^claude-auth: NOT-PERSISTED' \
   && printf '%s' "$worst_out" | grep -q '^claude-tmux-env: COLD-START-MISSING'; then
  ok "no-certification: the two fixtures really are the best- and worst-looking inputs"
else
  bad "no-certification: the fixtures did not reach opposite states, so the comparison below proves nothing: best=[$best_out] worst=[$worst_out]"
fi
if printf '%s\n%s' "$best_out" "$worst_out" | grep -q '^claude-auth: VERIFIED\|^claude-tmux-env: VERIFIED'; then
  bad "no-certification: a state named VERIFIED is still emitted: $(printf '%s\n%s' "$best_out" "$worst_out" | grep 'VERIFIED')"
else
  ok "no-certification: no input produces a VERIFIED state on either line"
fi
if printf '%s' "$best_out" | grep -q '^claude-auth-report: OBSERVATIONS-ONLY' \
   && printf '%s' "$worst_out" | grep -q '^claude-auth-report: OBSERVATIONS-ONLY'; then
  ok "no-certification: every report declares that neither line certifies this box"
else
  bad "no-certification: a report was printed with no scope note: best=[$best_out] worst=[$worst_out]"
fi
if [ "$best_rc" = "$worst_rc" ] && [ "$best_rc" -eq 0 ]; then
  ok "no-certification: the best and worst inputs are indistinguishable by exit status (both $best_rc)"
else
  bad "no-certification: the exit status still encodes a verdict (best=$best_rc worst=$worst_rc)"
fi

# =====================================================================================
# 35. AN ALTERNATE CREDENTIAL IN THE ENVIRONMENT IS REPORTED, NOT SILENTLY ABSORBED
#     (#3733, LIMITATION 2). The `--auth` probe scrubs CLAUDE_CODE_OAUTH_TOKEN and
#     re-supplies the PERSISTED value, and it does NOT scrub ANTHROPIC_API_KEY,
#     ANTHROPIC_AUTH_TOKEN, CLAUDE_CODE_USE_BEDROCK or CLAUDE_CODE_USE_VERTEX — so a
#     sentinel coming back proves that SOMETHING in the probe's environment authenticated,
#     not that the persisted value did. Deliberately DECLARED rather than scrubbed: the
#     ruling is that these are documented limitations of a report, not defects to patch.
#     What the report must therefore never do is claim the persisted value works.
#
#     THE STUB IS THE ORACLE: it answers ONLY when the alternate credential is present, and
#     the persisted token is a value it would otherwise reject. So a report claiming the
#     persisted credential works is affirmatively false on this input.
# =====================================================================================
d35=$(mkshim "$tmp/s35")
cat >"$d35/claude" <<'EOF'
#!/usr/bin/env bash
# Authenticates on the ALTERNATE credential alone; the persisted token is rejected.
if [ -n "${ANTHROPIC_API_KEY-}" ]; then printf 'CQLITE_CLAUDE_AUTH_OK\n'; exit 0; fi
printf 'Failed to authenticate\n'; exit 1
EOF
chmod +x "$d35/claude"
run_cap "$d35" "$ef2" 'ANTHROPIC_API_KEY=sk-ant-alternate-not-the-subject' -- --auth
if printf '%s' "$out" | grep -q '^claude-auth: PROBE-ANSWERED'; then
  ok "alternate credential: the probe answered, and the state says only that"
else
  bad "alternate credential: expected PROBE-ANSWERED, got: $out"
fi
if printf '%s' "$out" | grep '^claude-auth:' | grep -q 'ANTHROPIC_API_KEY'; then
  ok "alternate credential: the report NAMES the unscrubbed alternate credential it found"
else
  bad "alternate credential: an unscrubbed alternate credential went unreported: $out"
fi
# THE DIRECTION THAT MATTERS: the line must not assert that the PERSISTED value
# authenticated, because on this input it demonstrably did not.
# The verb is the whole assertion: `authenticated` names WHICH credential worked, and
# nothing here can observe that. The line must report that the run ANSWERED and stop there.
if printf '%s' "$out" | grep '^claude-auth:' | grep -qi 'authenticated\|credential is valid\|credential works'; then
  bad "alternate credential: the report claims the PERSISTED credential authenticated: $out"
else
  ok "alternate credential: the report does NOT claim the persisted credential authenticated"
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
# non-Linux branch in every case. Raised 91 -> 122 by round 4 (the digest identity of a
# delivered credential, the sudo-posture cases, and the bounding class), 122 -> 124 by
# round 5's two probe-working-directory interrupt cases, and 124 -> 151 by #3733's
# DEMOTION, the handover fix and the repair-status fix. Sections 34-36 (the
# no-certification invariant, the alternate-credential observation, the
# limitation-findability guard and the live/FIXED split), section 37 (the handover ordering,
# behavioural + structural + its positive control), section 38 (an explicitly requested
# repair that FAILED must red, and a SUCCESSFUL one must not), the seam-refusal exit-status
# pin, the marker-scanner positive control, and the assertions that changed subject where a
# verdict became an observation. 154 cases run, and the real-tmux isolation case
# (3 assertions) is still the only legitimately skippable one.
# THE FIGURE IS MEASURED, NOT COUNTED BY EYE, AND IT IS RE-MEASURED WHENEVER IT MOVES:
# forcing the tmux block's `command -v tmux` test to `true` in a throwaway `git worktree`
# reports 151/0/1. The value in this file is the authority — a figure quoted in a commit
# message is a snapshot of the run that produced it and does not follow later edits.
CASE_FLOOR=151
if [ "$((PASS + FAIL))" -lt "$CASE_FLOOR" ]; then
  printf 'FAIL - case floor: %s cases ran, expected at least %s (cases were lost)\n' "$((PASS + FAIL))" "$CASE_FLOOR"
  exit 1
fi
[ "$FAIL" -eq 0 ]
