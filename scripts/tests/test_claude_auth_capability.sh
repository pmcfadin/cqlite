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
#     claim the persisted value authenticated (section 35) — and on the FAILURE axis it
#     makes `FAILED` UNREACHABLE, because an accusation whose remedy is "replace the
#     persisted value" must be attributable to that value (section 47, both directions);
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

# THE ALTERNATE CREDENTIALS ARE SCRUBBED FROM EVERY RUNNER, AND THE LIST IS DERIVED (#3733).
# `claude` authenticates from ANTHROPIC_API_KEY, ANTHROPIC_AUTH_TOKEN, CLAUDE_CODE_USE_BEDROCK
# or CLAUDE_CODE_USE_VERTEX, and the library retains them BY DESIGN (LIMITATION 2) — which is
# why `FAILED` is unreachable while any is set. So every case that expects a MEASURED verdict
# about the persisted credential must run without them, or it passes (or reds) for a reason
# that is the invoking shell's environment and not the code: a fleet box exporting
# ANTHROPIC_API_KEY would turn every FAILED case in this suite into an UNMEASURED one.
# Scrubbed HERE, in the shared runners, rather than per case — the same idiom as SUDO_USER
# and CLAUDE_CONFIG_DIR above, and the reason is the same: a case that has to remember is a
# case that will forget. A case that WANTS an alternate passes it explicitly, which still
# wins, because `env` applies every `-u` before any assignment.
#
# DERIVED FROM THE LIBRARY'S OWN DECLARATION, never copied: a fifth alternate added there and
# not here would leave this suite scrubbing four of five. Sourcing is safe — the library's
# `main` runs only under its `BASH_SOURCE[0] = $0` guard — and it happens in a SUBSHELL so
# none of its globals reach the cases. A FAILED derivation is a NAMED REFUSAL and never an
# empty list: an empty scrub set is silently permissive in exactly the direction that matters.
# COUNTED IN A SCALAR, NOT `${#alt_scrub[@]}`: on bash 3.2 — a platform this repo supports
# and ships gtimeout/taskpolicy guards for — `${#arr[@]}` on an EMPTY array under `set -u`
# aborts the shell, which is exactly the derivation-failed path whose whole purpose is to
# print a NAMED refusal. A refusal that dies before its own message is not a refusal.
ALT_KEYS=$(. "$CAPLIB" >/dev/null 2>&1 && printf '%s' "${CLAUDE_AUTH_ALT_CRED_KEYS:-}")
alt_scrub=()
alt_n=0
for alt_k in $ALT_KEYS; do
  case "$alt_k" in
    ''|*[!A-Za-z0-9_]*) printf 'test_claude_auth_capability: REFUSING TO RUN (alt-key-derivation): the library declared an alternate credential name this harness will not scrub: %s\n' "'$alt_k'" >&2; exit 1 ;;
  esac
  alt_scrub+=(-u "$alt_k")
  alt_n=$((alt_n + 1))
done
if [ "$alt_n" -lt 4 ]; then
  printf 'test_claude_auth_capability: REFUSING TO RUN (alt-key-derivation): CLAUDE_AUTH_ALT_CRED_KEYS yielded %s names (%s); the suite cannot neutralise what it cannot read.\n' \
    "$alt_n" "'${ALT_KEYS:-<empty>}'" >&2
  exit 1
fi

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
  # CLAUDE_CONFIG_DIR IS SCRUBBED BY DEFAULT TOO, and on this fleet that is not theoretical:
  # the host exports it (`/data/auth/claude`), and the repair falls back to THIS PROCESS's
  # value when the pam_env file names none — so a case about "the config dir could not be
  # seeded from anywhere" silently took the inherited-value branch and tested the opposite
  # thing. Cases that want it set pass it explicitly, which still wins: `env` applies `-u`
  # before assignments.
  out=$(PATH="$shimdir:$PATH" env -u SUDO_USER -u SUDO_UID -u CLAUDE_CONFIG_DIR \
        "${alt_scrub[@]}" \
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
#   fiforesult  — no server, and the would-be server replaces the pane's REPORT PATH with a
#                 FIFO whose writer emits one complete report (satisfying the bounded wait
#                 loop, which CONSUMES it — a fifo is not seekable) and then holds the fifo
#                 open without writing again. So every LATER open of that path blocks
#                 forever. This is the post-handover substitution hazard made deterministic:
#                 a same-uid peer lane can do exactly this to the handed-over directory
#                 (#3733 F1).
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
      fiforesult)
        # The command the probe passes is `sh 'DIR/probe.sh' 'RES'` — a fixed shape, since
        # the probe REFUSES a directory containing a quote or whitespace. Take the last
        # single-quoted word as the report path.
        __r="\${cmd##* \'}"; __r="\${__r%\'}"
        rm -f "\$__r"
        mkfifo "\$__r" || exit 1
        # ONE complete report, then hold the writer open WITHOUT writing again. The wait
        # loop's `grep` consumes the bytes; every read after it blocks on an empty fifo.
        # Bounded by `sleep` so a killed test leaves nothing running for long.
        { printf 'tok=set\ntoklen=%s\ntokdig=deadbeef\ncfg=%s\nend\n' "\${#0}" '$cfg'; sleep 45; } >"\$__r" &
        exit 0 ;;
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
      no-server|probefail|substitute|slowstart|fiforesult) printf 'no server running on %s/tmux-1000/default\n' "\${TMPDIR:-/tmp}" >&2; exit 1 ;;
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
# THE SUBJECT IS NO LONGER THE PROBE DIRECTORY. Since the handover was NARROWED (#3733) the
# subjects are the report FILE and the `sock/` subdirectory, so a probe-dir-only check would
# silently stop measuring. Look for probe.sh beside the subject AND one level up, which
# covers a file subject, a subdirectory subject, and the old whole-directory subject alike.
if [ -n "\$subj" ]; then
  st=absent
  for c in "\$subj/probe.sh" "\$(dirname "\$subj")/probe.sh"; do
    [ -e "\$c" ] && st=present
  done
  printf 'at-handover probe.sh=%s\n' "\$st" >>"$d/chown-calls.log"
fi
exit $crc
EOF
  chmod +x "$d/chown"
}

# plant_chmod <dir>: a recording `chmod` that also DOES the chmod, so the probe still works.
# It records the mode and the subject, never a path-free summary: the #3733 handover is a set
# of NARROW grants and which subject got which mode is the whole property.
plant_chmod() {
  local d="$1"
  cat >"$d/chmod" <<EOF
#!/usr/bin/env bash
printf 'chmod %s\n' "\$*" >>"$d/chmod-calls.log"
exec /bin/chmod "\$@"
EOF
  chmod +x "$d/chmod"
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
  PATH="$d18:$PATH" env "${alt_scrub[@]}" CQLITE_BOOTSTRAP_TEST_MODE=1 CQLITE_BOOTSTRAP_SKIP_GATE_PIN=1 \
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
# run_bootstrap_root <root> <shimdir> <envfile> <args...>: as run_bootstrap_in, but against
# a CALLER-CHOSEN sandbox root, so a case can drive a root whose capability script is absent.
# The root is always a COPY under $tmp — this suite never makes its own checkout unreadable.
run_bootstrap_root() {
  local rt="$1" sd="$2" evf="$3"; shift 3
  PATH="$sd:$PATH" env "${alt_scrub[@]}" CQLITE_BOOTSTRAP_TEST_MODE=1 CQLITE_BOOTSTRAP_SKIP_GATE_PIN=1 \
    CQLITE_CLAUDE_AUTH_ENV_FILE="$evf" HOME="$tmp/home" \
    CQLITE_PROJECT_ACCOUNT="$BS_ACCOUNT" CQLITE_PROJECT_OWNER=pmcfadin CQLITE_PROJECT_NUMBER=1 \
    bash "$rt/scripts/bootstrap-agent-machine.sh" "$@" 2>&1
}
run_bootstrap_in() {
  local sd="$1" evf="$2"; shift 2
  PATH="$sd:$PATH" env "${alt_scrub[@]}" CQLITE_BOOTSTRAP_TEST_MODE=1 CQLITE_BOOTSTRAP_SKIP_GATE_PIN=1 \
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
# 39. A PARTIAL REPAIR MUST NOT REPORT SUCCESS, AND MUST SAY WHICH HALF WORKED (#3733 F2).
#     `claude_auth_fix_tmux_env` seeds the TOKEN, then the CONFIG DIR. When the config dir
#     is in NEITHER the pam_env file NOR this process's environment it printed
#     "could NOT be seeded" and `return 0` — so `--fix-tmux-env` exited 0 on a repair that
#     half happened, and bootstrap's F1 check (which reads that status) would have called it
#     complete. TOKEN-SEEDED-BUT-NO-CONFIG-DIR IS PRECISELY THE UN-ONBOARDED PICKER STATE
#     THIS ISSUE EXISTS FOR: the credential authenticates and `claude` still lands on the
#     first-run chooser (fact 2), so reporting it as success is the worst available answer.
#     SAME LICENCE AS F1: this is a verdict about whether an ACTION THE OPERATOR REQUESTED
#     completed, not about whether the credential is valid. The first is observable from the
#     action's own outcome; the second is what the #3733 demotion removed.
#     THE OPERATOR NEEDS BOTH FACTS, so the assertions are on the TEXT as well as the status:
#     the token half WORKED and the config half did NOT. A bare non-zero would send someone
#     to re-run a seed that already succeeded.
# =====================================================================================
# $ef_nocfg carries a token and NO CLAUDE_CONFIG_DIR; run_cap scrubs the inherited one (see
# the note there — the host really does export it), so the repair has no source for it.
d39=$(mkshim "$tmp/s39"); plant_tmux "$d39" missing
run_cap "$d39" "$ef_nocfg" -- --fix-tmux-env
# THE FIXTURE MUST HAVE REACHED THE PARTIAL STATE, asserted first: if the token half had
# also failed, or a config dir had been found, the status assertion below has another cause.
if grep -q 'seeded CLAUDE_CODE_OAUTH_TOKEN into the running tmux server' <<<"$out" \
   && grep -q 'CLAUDE_CONFIG_DIR could NOT be seeded' <<<"$out"; then
  ok "partial repair: the fixture really is the partial case (token seeded, config dir sourceable from nowhere)"
else
  bad "partial repair: the fixture is not the partial case, so the status assertion below has another cause: $out"
fi
if [ "$rc" -ne 0 ]; then
  ok "partial repair: --fix-tmux-env exits NON-ZERO when only half the requested repair happened"
else
  bad "partial repair: --fix-tmux-env exited 0 on a half-done repair — token seeded, no config dir, which IS the un-onboarded picker state"
fi
# AND IT REPORTS WHICH HALF, both directions, because the remedy depends on it: re-running
# the seed is pointless and the operator has to know only the config dir is outstanding.
if grep -q 'the CLAUDE_CODE_OAUTH_TOKEN half of this repair SUCCEEDED' <<<"$out"; then
  ok "partial repair: the failure line states that the token half already succeeded"
else
  bad "partial repair: the failure line does not say the token was already seeded, so the operator cannot tell which half is outstanding: $(printf '%s' "$out" | grep 'could NOT be seeded')"
fi
# NOT A BLANKET NON-ZERO: the FULL repair must still exit 0, or F1's status check would call
# every successful repair a failure. $ef2 names a config dir, so both halves complete.
d39b=$(mkshim "$tmp/s39b"); plant_tmux "$d39b" missing
run_cap "$d39b" "$ef2" -- --fix-tmux-env
if [ "$rc" -eq 0 ] && grep -q 'seeded CLAUDE_CONFIG_DIR=' <<<"$out"; then
  ok "partial repair: a COMPLETE repair still exits 0 (the non-zero is about the missing half, not about seeding)"
else
  bad "partial repair: a complete repair no longer exits 0 (rc=$rc) — F1's status check would call every success a failure: $out"
fi

# =====================================================================================
# 40. THE PANE REPORT IS READ ONCE, UNDER THE BOUND (#3733 F1, BLOCKER).
#     THE HAZARD. The report lived at a path inside the directory the probe has ALREADY
#     `chown`ed to the invoking user, and it was read by FOUR unbounded `sed … | tail -1`
#     calls. On this fleet every lane runs as ONE user, so a peer lane can replace that path
#     between the bounded wait and those reads — and **replacing it with a FIFO makes root's
#     `sed` block forever**: an unattended provisioning run hangs with NO verdict at all.
#     This repo already names that exact shape as grounds for a fatal refusal elsewhere
#     ("opening one BLOCKS FOREVER, which is a verdict-less stall in an unattended lane").
#     IT SUPERSEDES A DECLARED RESIDUAL, and that is the lesson worth keeping. The old note
#     argued the read was safe because the bounded wait loop had already seen the `end` line,
#     so the file was "complete, regular and small by the time it is read". That argues about
#     what the file WAS THEN, not about what the path RESOLVES TO NOW — the same shape as
#     every other finding on this issue: a check placed before the harm cannot bound it.
#     AND FOUR OPENS ARE FOUR CHANCES TO READ A MUTATED FILE, so today's record could be
#     internally inconsistent with no attacker at all. Hence ONE bounded read into memory,
#     then parse in-process, and the terminating marker is re-required on the captured bytes.
#     BEHAVIOURAL, NOT STRUCTURAL — the substitution is made deterministic rather than raced:
#     the `fiforesult` stub emits one complete report through a FIFO (the wait loop's `grep`
#     CONSUMES it, since a fifo is not seekable) and then holds the writer open without
#     writing again, so every later open of that path blocks forever. Under the old code the
#     run hangs and only an OUTER timeout ends it; under the fix the probe's own bound fires
#     and it reports a named cause. Costs ~20s of wall clock: that is the bound doing its job.
# =====================================================================================
d40=$(mkshim "$tmp/s40"); plant_tmux "$d40" fiforesult
f40_t0=$(date +%s)
f40_rc=0
# INVOKED WITH AN OUTER `timeout` rather than through run_cap, because the RED state of this
# case is a HANG: without an outer bound the suite would stall instead of failing, and a
# stalled suite is not a red. The outer bound is generous relative to the probe's own 20s, so
# firing it means the probe did not bound itself.
f40_out=$(timeout 75 env -u SUDO_USER -u SUDO_UID -u CLAUDE_CONFIG_DIR \
      PATH="$d40:$PATH" CQLITE_BOOTSTRAP_TEST_MODE=1 CQLITE_CLAUDE_AUTH_ENV_FILE="$ef2" \
      bash "$CAPLIB" --tmux-env 2>&1) || f40_rc=$?
f40_elapsed=$(( $(date +%s) - f40_t0 ))
printf '%s\n' "$f40_out" >>"$TRANSCRIPT"
# THE FIXTURE MUST HAVE PLANTED ITS FIFO, asserted first: if `mkfifo` was unavailable or the
# path parse failed, the stub falls through and this case measures an ordinary probe.
if [ -f "$d40/tmux-calls.log" ] && grep -q '^new-session ' "$d40/tmux-calls.log"; then
  ok "bounded report read: the fifo fixture reached new-session (the substitution was planted)"
else
  bad "bounded report read: the fifo fixture never started a probe, so this case measures nothing: $(cat "$d40/tmux-calls.log" 2>/dev/null)"
fi
if [ "$f40_rc" != 124 ] && [ "$f40_rc" != 137 ]; then
  ok "bounded report read: the run ENDED ITSELF (${f40_elapsed}s) — the outer timeout never had to fire"
else
  bad "bounded report read: the run had to be killed by the OUTER timeout after ${f40_elapsed}s — an unbounded read of the report path blocks forever, which is a verdict-less stall in an unattended lane"
fi
if grep -q '^claude-tmux-env: ' <<<"$f40_out"; then
  ok "bounded report read: it still produced an observation line rather than dying silently"
else
  bad "bounded report read: no observation line was printed at all: $f40_out"
fi
# AND THE CAUSE NAMES THE READ, not the pane: "the pane did not report" would send the reader
# to tmux for a path that was substituted underneath us.
if grep '^claude-tmux-env: ' <<<"$f40_out" | grep -qi 'report could not be read\|reading the isolated pane'; then
  ok "bounded report read: the cause names the READ that was bounded, not a pane that never answered"
else
  bad "bounded report read: the cause misattributes a substituted report path: $(grep '^claude-tmux-env: ' <<<"$f40_out")"
fi

# =====================================================================================
# 41. AN UNKNOWN OR EXTRA ARGUMENT IS REFUSED, NOT IGNORED (#3733 F2).
#     `--auth --typo` silently ran the REAL, BILLED `claude -p` probe while the operator
#     believed they had typed something else — a silent spend plus a report they will misread
#     as being about the flag they meant. An unknown FIRST argument was already refused; what
#     was ignored was everything AFTER a recognised mode.
#     THE STATUS IS 2 BECAUSE THE CONTRACT ALREADY PROMISES 2, verified rather than invented:
#     the usage text says "For the three report modes the ONLY non-zero status is a usage
#     error (2)". So this honours a promise that was already made and not kept, and the
#     contract text needs no change.
#     IT CANNOT COLLIDE WITH THE REPORT MODES' DELIBERATE `return 0`: the refusal happens
#     BEFORE any report is printed, so there is no run in which a printed report carries a
#     non-zero status. That `return 0` exists so no exit status can become the certification
#     #3733 removed, and a usage error is not a verdict about the box.
# =====================================================================================
# EVERY MODE, TABLE-DRIVEN, so a mode added later without a length check is visible as a
# missing row rather than as silence. `probe-must-not-run` is the point of the case: the
# `claude` stub here RECORDS being invoked, and a refusal must leave that record empty.
d41=$(mkshim "$tmp/s41")
cat >"$d41/claude" <<EOF
#!/usr/bin/env bash
printf 'invoked\n' >>"$d41/claude-invocations.log"
printf '%s\n' '$SENTINEL'
EOF
chmod +x "$d41/claude"
plant_tmux "$d41" complete
a41_bad=''
for a41_case in \
  '--auth --typo' \
  '--auth --show-probe-output extra' \
  '--tmux-env extra' \
  '--report extra' \
  '--fix-tmux-env extra' \
  '--help extra'
do
  # shellcheck disable=SC2086  # deliberate word-split: the case IS an argument vector
  run_cap "$d41" "$ef2" -- $a41_case
  [ "$rc" = 2 ] || a41_bad="${a41_bad:+$a41_bad; }[$a41_case]=rc$rc"
done
if [ -z "$a41_bad" ]; then
  ok "argument validation: every unknown or extra argument is refused with status 2"
else
  bad "argument validation: these invocations were not refused with 2: $a41_bad"
fi
# THE BILLED PROBE MUST NOT HAVE RUN. This is the cost the finding is about, and a status
# assertion alone would not see it: a refusal that still probed would pass the check above.
if [ ! -f "$d41/claude-invocations.log" ]; then
  ok "argument validation: no refused invocation ran the real (billed) claude probe"
else
  bad "argument validation: a refused invocation still ran the claude probe $(grep -c . "$d41/claude-invocations.log") time(s) — the operator is billed for a command they mistyped"
fi
# AND THE VALID FORMS MUST STILL WORK — a refusal that rejected the documented spellings
# would be the guard that reds on correct input.
run_cap "$d41" "$ef2" -- --auth --show-probe-output
if [ "$rc" = 0 ] && grep -q '^claude-auth: ' <<<"$out"; then
  ok "argument validation: the documented '--auth --show-probe-output' form still runs and exits 0"
else
  bad "argument validation: the valid --show-probe-output form was refused (rc=$rc): $out"
fi
run_cap "$d41" "$ef2" -- --help
if [ "$rc" = 0 ] && grep -q 'usage:' <<<"$out"; then
  ok "argument validation: bare --help still works and exits 0"
else
  bad "argument validation: --help regressed (rc=$rc)"
fi

# =====================================================================================
# 42. A REQUESTED REPAIR THAT COULD NOT EVEN BE ATTEMPTED IS AN ACTION FAILURE (#3733).
#     The `[ ! -r "$CLAUDE_AUTH_LIB" ]` branch printed `claude-auth: UNREPORTED` through
#     `info` and never consulted FIX_CLAUDE_AUTH — so `--fix-claude-auth` with the capability
#     script missing or unreadable meant the operator's explicitly requested repair silently
#     did not happen and the run could still exit 0, `--strict` included. Third instance of
#     one family on this branch (the discarded pipeline status, the half-done seed, and now a
#     repair that never ran at all): AN ACTION THE OPERATOR REQUESTED VANISHED WITHOUT
#     AFFECTING THE STATUS.
#     THE LICENCE IS THE ONE ALREADY ESTABLISHED, and the boundary is narrow. The OBSERVATION
#     lines stay `info`: nothing is certified, so there is no green to buy, and a `warn` there
#     would make `--strict` fail on a line the #3733 ruling says is not a verdict. What is a
#     legitimate verdict is whether an ACTION completed — observable from the action's own
#     outcome. So the UNREPORTED line is untouched and a REQUESTED-repair-impossible case is
#     added beside it.
#     HOW THE STATUS IS ASSERTED, stated plainly because a proxy here would be the very
#     defect: in bootstrap `warn()` IS the mechanism `--strict` reads (it increments the
#     counter `--strict` exits 1 on). So the attributable assertion is the warn-count DELTA
#     between the same sandbox with and without `--fix-claude-auth` — a bare `--strict` exit
#     code is NOT attributable here, because this sandbox has ambient warnings (an
#     origin-less root, the push-probe opt-out) that red it either way.
# =====================================================================================
# A ROOT WITH NO CAPABILITY SCRIPT. A copy under $tmp with the library removed — the same
# `[ ! -r ]` branch an unreadable one takes, without leaving a mode-000 file behind.
bs_nolib="$tmp/bs-root-nolib"; mkroot "$bs_nolib"; rm -f "$bs_nolib/scripts/claude-auth-capability.sh"
d42=$(mkshim "$tmp/s42"); plant_bootstrap_quiet_stubs "$d42"
bs42fix=$(run_bootstrap_root "$bs_nolib" "$d42" "$ef2" --skip-smoke --skip-push-probe --fix-claude-auth)
bs42no=$(run_bootstrap_root "$bs_nolib" "$d42" "$ef2" --skip-smoke --skip-push-probe)
printf '%s\n%s\n' "$bs42fix" "$bs42no" >>"$TRANSCRIPT"
# THE FIXTURE MUST HAVE REACHED THE MISSING-LIBRARY BRANCH, asserted first.
if grep -q 'claude-auth: UNREPORTED' <<<"$bs42fix"; then
  ok "requested repair impossible: the fixture reached the missing-capability-script branch"
else
  bad "requested repair impossible: the fixture did not reach the UNREPORTED branch, so this case tests nothing: $(printf '%s' "$bs42fix" | sed -n '/Claude credential/,/^$/p' | head -6)"
fi
if bs_marked "$bs42fix" warn 'claude-auth' >/dev/null; then
  ok "requested repair impossible: --fix-claude-auth with no capability script emits a [warn]"
else
  bad "requested repair impossible: the requested repair vanished silently — no [warn], so bootstrap can exit 0 under --strict: $(printf '%s' "$bs42fix" | sed -n '/Claude credential/,/^$/p' | head -8)"
fi
# THE NO-FIX PATH IS UNCHANGED: an observation that could not be produced is not a failure.
if bs_marked "$bs42no" warn 'claude-auth' >/dev/null; then
  bad "requested repair impossible: a run that requested NO repair now warns about the missing script — that turns a non-verdict line into one --strict fails on: $(bs_marked "$bs42no" warn 'claude-auth')"
else
  ok "requested repair impossible: with no repair requested the UNREPORTED line stays a non-verdict (no [warn])"
fi
w42f=$(bs_marked "$bs42fix" 'ok|warn' '.' 2>/dev/null | grep -c '\[warn\]')
w42n=$(bs_marked "$bs42no" 'ok|warn' '.' 2>/dev/null | grep -c '\[warn\]')
if [ "$w42f" -eq "$((w42n + 1))" ]; then
  ok "requested repair impossible: the requesting run carries EXACTLY ONE more [warn] ($w42f vs $w42n), which is what --strict counts"
else
  bad "requested repair impossible: the warning counts differ by $((w42f - w42n)), not 1 (requested=$w42f not-requested=$w42n) — the red is not attributable to the vanished repair"
fi

# =====================================================================================
# 43. OPT-OUT PLUS --fix-claude-auth IS ALREADY DECIDED — BOTH WAYS — AND MUST SAY SO.
#     NOT AMBIGUOUS, and this pins it rather than re-litigating it. The flag-parsing block
#     resolves the pair explicitly and the reasoning is at that site:
#       * an EXPLICIT `--skip-claude-auth` beside `--fix-claude-auth` is a USAGE ERROR,
#         exit 2 (already pinned by the contradictory-flags case in section 18) — two
#         explicit, opposite intents must not resolve silently;
#       * the ENV spelling `CQLITE_BOOTSTRAP_SKIP_CLAUDE_AUTH=1` is the WEAKER signal, so an
#         explicit `--fix-claude-auth` OVERRIDES it — a harness exporting the opt-out on a
#         fixed command line must not be able to neuter a caller's explicit repair.
#     SO THE SECTION'S OPT-OUT BRANCH CAN NEVER WIN OVER AN EXPLICIT `--fix-claude-auth`.
#     What WAS missing is that the override happened SILENTLY: the operator opted out via the
#     environment, asked for a repair, and got one with nothing saying which intent lost.
#     That is the same "a requested action's fate is unstated" shape as section 42, so it is
#     reported rather than left implicit.
# =====================================================================================
d43=$(mkshim "$tmp/s43"); plant_bootstrap_quiet_stubs "$d43"
plant_claude_probe_env "$d43"; plant_tmux_stateful "$d43" "$TOK_OTHER" "$CFGDIR"
bs43=$(CQLITE_BOOTSTRAP_SKIP_CLAUDE_AUTH=1 run_bootstrap_in "$d43" "$ef2" --skip-smoke --skip-push-probe --fix-claude-auth)
printf '%s\n' "$bs43" >>"$TRANSCRIPT"
if grep -q 'claude-auth: OPT-OUT' <<<"$bs43"; then
  bad "opt-out override: the env opt-out won over an EXPLICIT --fix-claude-auth — a harness can neuter a caller's repair: $(printf '%s' "$bs43" | grep 'claude-auth: OPT-OUT')"
else
  ok "opt-out override: an explicit --fix-claude-auth overrides the ENV opt-out (the section ran)"
fi
if grep -q 'claude-tmux-env: SERVER-CARRIES-BOTH' <<<"$bs43"; then
  ok "opt-out override: ...and the repair it asked for actually happened"
else
  bad "opt-out override: the override reported no completed repair: $(printf '%s' "$bs43" | grep 'claude-tmux-env:')"
fi
if grep -qi 'opt-out.*overrid\|overrid.*opt-out' <<<"$bs43"; then
  ok "opt-out override: the run STATES that the env opt-out was overridden by the explicit flag"
else
  bad "opt-out override: the override is SILENT — the operator cannot tell which of their two intents lost: $(printf '%s' "$bs43" | sed -n '/Claude credential/,/^$/p' | head -6)"
fi

# =====================================================================================
# 44. THE SUBSTITUTION CAPABILITY IS REMOVED, NOT DETECTED (#3733).
#     TWO CONSECUTIVE ROUNDS LANDED IN ONE MECHANISM: root reading a path inside a directory
#     it had handed away. Round 428 — unbounded `sed` on a FIFO planted there — HUNG an
#     unattended run forever. Round 430 — the bounded `cat` that fixed it — accumulated
#     memory from a symlink to /dev/zero until the bound fired, i.e. bounded in TIME and
#     unbounded in MEMORY. Carving the READ a third time is the move this repo rules against;
#     it is #3312's umbrella lesson on a filesystem path instead of a text channel: REMOVE
#     the shared channel, do not pick a rarer delimiter.
#     THE ENABLING CONDITION WAS `chown -R`, WHICH GRANTS DIRECTORY WRITE — i.e.
#     unlink/create/symlink. The delegate never needed it: the pane TRUNCATES the report
#     (write on the FILE, whose inode root pre-creates), and the only thing that must create
#     a NEW entry is tmux's socket. So `$__dir` stays root-owned at 0711 (traverse, no write,
#     not even LIST), only the report FILE's ownership moves, and the socket gets its own
#     delegate-owned `sock/`.
#     HOW THE REFUSAL IS ASSERTED WITHOUT ROOT, in two halves, because this suite runs
#     unprivileged and CANNOT create a root-owned directory to attack:
#      (a) THE OS MECHANISM, measured on this host against a real directory that is
#          root-owned and not writable by us: a non-owner CANNOT create or unlink an entry
#          there. That is the property the fix rests on, and it is measured rather than
#          asserted from the manual page.
#      (b) THE POLICY, measured from the probe's RUN-TIME argv through recording stubs: the
#          probe never chowns the DIRECTORY, sets it to exactly that traverse-only mode, and
#          hands over only the report file and the socket subdirectory.
#     (a) AND (b) TOGETHER ARE THE REFUSAL: the mode the code actually sets is one the OS
#     actually refuses entry-creation under. Neither half alone would be evidence — (a) is
#     about a directory that is not ours, (b) is about a mode with no demonstrated effect.
#     A real end-to-end plant needs root; it was run out of band against this exact code and
#     recorded in the commit message, deliberately NOT added here: a sudo-dependent case
#     would leave root-owned litter that this suite's own `rm -rf` trap cannot remove.
# =====================================================================================
# (a) THE OS MECHANISM. Any root-owned directory that we cannot write serves; `/usr` exists
#     on every platform this repo supports. If we CAN write it (the suite running as root),
#     the mechanism cannot be measured here and that is a SKIP with its reason, never a pass.
if [ -d /usr ] && [ ! -w /usr ] && [ -x /usr ]; then
  m44a=0; m44b=0
  ( : > /usr/cqlite-3733-probe ) 2>/dev/null && m44a=1
  ( ln -s /dev/zero /usr/cqlite-3733-link ) 2>/dev/null && m44b=1
  rm -f /usr/cqlite-3733-probe /usr/cqlite-3733-link 2>/dev/null
  if [ "$m44a" = 0 ] && [ "$m44b" = 0 ]; then
    ok "substitution removed (os mechanism): a non-owner can neither create a file nor plant a symlink in a root-owned directory it cannot write"
  else
    bad "substitution removed (os mechanism): entry creation in a non-writable root-owned directory SUCCEEDED (file=$m44a symlink=$m44b) — the permission model the fix rests on does not hold on this host"
  fi
else
  skip "substitution removed (os mechanism)" "no root-owned directory this process cannot write (running as root?)"
fi

# (b) THE POLICY, from run-time argv. Delegated cold-start posture, recording chown + chmod.
d44=$(mkshim "$tmp/s44"); plant_tmux "$d44" no-server; plant_id "$d44" root 0 "$INVOKER" 4711
plant_delegator "$d44" runuser; plant_chown "$d44" 0; plant_chmod "$d44"
run_cap "$d44" "$ef2" "SUDO_USER=$INVOKER" "SUDO_UID=4711" -- --tmux-env
# THE FIXTURE MUST HAVE PERFORMED A HANDOVER, asserted first, or every negative below is
# satisfied by a probe that never got that far.
if [ -f "$d44/chown-calls.log" ] && [ -f "$d44/chmod-calls.log" ] \
   && grep -q '^claude-tmux-env: COLD-START-DELIVERS-BOTH' <<<"$out"; then
  ok "substitution removed (policy): the delegated probe ran its handover and still reported a delivery"
else
  bad "substitution removed (policy): the fixture did not complete a delegated handover, so the assertions below have no subject: out=[$out] chown=[$(cat "$d44/chown-calls.log" 2>/dev/null)] chmod=[$(cat "$d44/chmod-calls.log" 2>/dev/null)]"
fi
# THE DIRECTORY ITSELF IS NEVER CHOWNED, and `-R` never appears. Either would restore
# directory write to the delegate and with it the whole substitution capability.
if grep -qE '^chown .*(-R|--recursive)' "$d44/chown-calls.log" 2>/dev/null; then
  bad "substitution removed (policy): a RECURSIVE chown is back — that grants directory write and re-enables substitution: $(grep '^chown ' "$d44/chown-calls.log")"
elif grep -qE '^chown [^ ]+ [^ ]*cqlite-tmux-probe\.[A-Za-z0-9]+$' "$d44/chown-calls.log" 2>/dev/null; then
  bad "substitution removed (policy): the probe DIRECTORY itself was chowned to the delegate — that grants directory write: $(grep '^chown ' "$d44/chown-calls.log")"
else
  ok "substitution removed (policy): neither a recursive chown nor a chown of the probe directory occurs"
fi
# THE HANDED-OVER SUBJECTS ARE EXACTLY THE REPORT FILE AND THE SOCKET SUBDIRECTORY.
if grep -qE '^chown [^ ]+ .*/result$' "$d44/chown-calls.log" 2>/dev/null \
   && grep -qE '^chown [^ ]+ .*/sock$' "$d44/chown-calls.log" 2>/dev/null; then
  ok "substitution removed (policy): ownership moves for the report FILE and the socket subdirectory only"
else
  bad "substitution removed (policy): the narrow grants are not what ran: $(cat "$d44/chown-calls.log" 2>/dev/null)"
fi
# AND THE DIRECTORY IS SET TO THE TRAVERSE-ONLY MODE (a) just proved refuses creation. The
# mode is matched EXACTLY: `0711` grants group/other traverse and no write, which is the
# property. A pattern like `07*` would accept `0777`.
if grep -qE '^chmod 0711 [^ ]*cqlite-tmux-probe\.[A-Za-z0-9]+$' "$d44/chmod-calls.log" 2>/dev/null; then
  ok "substitution removed (policy): the probe directory is set to 0711 — traverse only, no write, not listable"
else
  bad "substitution removed (policy): the probe directory was not set to a traverse-only mode: $(cat "$d44/chmod-calls.log" 2>/dev/null)"
fi

# =====================================================================================
# 45. OUTPUT-VARIABLE ASSIGNMENT NEVER GOES THROUGH `eval` (#3733).
#     WHAT WAS ACTUALLY WRONG, stated precisely because the review's description and the
#     measurement disagree and the measurement wins. The finding said probe output and tmux
#     STDERR were "interpolated into strings that eval then reparses", so crafted text would
#     execute. MEASURED ON THE EXACT SHIPPED CONSTRUCT, that is NOT reproducible: every one
#     of the 118 sites used the DEFERRED form (`\$var` / `\$(cmd)` inside the eval string),
#     and bash does NOT re-parse an expansion's RESULT — the payload is stored LITERALLY.
#     A scan of the pre-fix file found ZERO sites with an unescaped expansion in the RHS,
#     which is the form that would execute. So the VALUE side was not a live vector.
#     WHAT *IS* REAL, AND IS WHAT THIS CASE PINS: the NAME side. `eval "$__ov=…"` expands
#     `$__ov` BEFORE eval parses, at all 118 sites, so a crafted out-var NAME executes.
#     Proven through the real public function against both versions: the old code RAN the
#     payload, the new code refuses it. Today every caller passes a literal name, so it was
#     LATENT rather than live — one refactor away, and worth removing rather than trusting.
#     WHY THE SWEEP IS STILL THE RIGHT FIX rather than patching the name side: it removes the
#     CONSTRUCT, so the value-side form that WOULD execute (an unescaped interpolation, which
#     nothing in this file prevented and which the older backtick/`$(` guard does not catch)
#     cannot be written into a new site at all.
# =====================================================================================
# (a) BEHAVIOURAL, WITH A REAL SIDE EFFECT — not a string comparison. The payload would
#     `touch` a canary; the assertion is that the canary does not exist AND the call refused.
inj_canary="$tmp/injection-canary"
rm -f "$inj_canary"
inj_err=$( . "$CAPLIB"; claude_auth_read_key_into "z=1; touch '$inj_canary'; e" inj_st "$ef2" CLAUDE_CODE_OAUTH_TOKEN 2>&1 >/dev/null )
if [ ! -e "$inj_canary" ]; then
  ok "no eval assignment: a crafted out-var NAME does not execute (no canary was created)"
else
  bad "no eval assignment: a crafted out-var NAME EXECUTED its payload — the canary exists"
  rm -f "$inj_canary"
fi
# THE REFUSAL IS ANNOUNCED, and stderr is what is asserted rather than an exit status.
# DECLARED RESIDUAL, measured while writing this case: the refusal's rc 2 does NOT propagate
# out of the public wrapper — the helper is called as the last statement of a branch that
# then `return`s its own status, at 118 sites. It is left that way deliberately: a
# non-identifier out-var name is a PROGRAMMING error (every caller in the file passes a
# literal name or a positional holding one), not a runtime condition, so threading a status
# through 118 call sites would carry real regression risk for a path no caller can reach.
# What must not happen is that it is SILENT, so the loud stderr line is the assertion.
if printf '%s' "$inj_err" | grep -q 'REFUSING: assignment target is not a plain variable name'; then
  ok "no eval assignment: ...and the refusal is ANNOUNCED on stderr rather than landing somewhere unintended"
else
  bad "no eval assignment: a non-identifier assignment target was accepted silently — stderr was [$inj_err]"
fi
# ...and the ordinary path still assigns, or the refusal would be a guard that reds on
# correct input. Asserted through the same public function on a real name.
inj_ok=$( . "$CAPLIB"; claude_auth_read_key_into inj_v inj_s "$ef2" CLAUDE_CODE_OAUTH_TOKEN; printf '%s' "$inj_s" )
if [ "$inj_ok" = present ]; then
  ok "no eval assignment: a legitimate out-var name still receives its value"
else
  bad "no eval assignment: the ordinary assignment path regressed (state=[$inj_ok])"
fi

# (b) STRUCTURAL, AS A CLOSED SET. Not "no eval assignments" but "there is EXACTLY ONE eval
#     and it is the trap restore" — a closed set makes a NEW eval of any shape require a
#     deliberate update here, where "no assignments" would silently admit other constructs.
#     The trap restore is exempt for a stated reason: `trap -p` emits ready-to-re-execute
#     COMMANDS that bash itself quoted, there is no `printf -v` equivalent for a command, and
#     its input is the CALLER's own traps — invoker-class, unreachable by a peer lane,
#     `claude` or tmux.
inj_evals=$(grep -n 'eval ' "$CAPLIB" | grep -vE ':[[:space:]]*#' || true)
inj_n=$(printf '%s\n' "$inj_evals" | grep -c . || true)
inj_assign=$(printf '%s\n' "$inj_evals" | grep -E 'eval "\$([A-Za-z_][A-Za-z0-9_]*|[0-9]+)=' || true)
if [ -n "$inj_assign" ]; then
  bad "no eval assignment: an eval-based assignment to a caller-named variable is back: $inj_assign"
elif [ "$inj_n" != 1 ]; then
  bad "no eval assignment: expected EXACTLY ONE eval (the trap restore), found $inj_n — a new eval needs a stated justification here: $inj_evals"
elif printf '%s\n' "$inj_evals" | grep -q 'CLAUDE_AUTH_PROBE_PREV_TRAPS'; then
  ok "no eval assignment: the file holds exactly one eval and it is the justified trap restore"
else
  bad "no eval assignment: the single remaining eval is not the trap restore: $inj_evals"
fi
# THE POSITIVE CONTROL. Every assertion above is negative, so a scanner that matched nothing
# would green them all. Plant an eval assignment in a COPY and require the scan to find it.
inj_copy="$tmp/caplib-eval-planted.sh"
{ printf 'eval "$out=\\"planted\\""\n'; cat "$CAPLIB"; } >"$inj_copy"
if grep -n 'eval ' "$inj_copy" | grep -vE ':[[:space:]]*#' \
     | grep -qE 'eval "\$([A-Za-z_][A-Za-z0-9_]*|[0-9]+)=' \
   && ! printf '%s\n' "$inj_evals" | grep -qE 'eval "\$([A-Za-z_][A-Za-z0-9_]*|[0-9]+)='; then
  ok "no eval assignment: the scan FINDS a planted eval assignment (so the clean verdict is not vacuous)"
else
  bad "no eval assignment: the scan cannot see a planted eval assignment — it proves nothing about the real file"
fi

# =====================================================================================
# 46. THE REPAIR DISPATCH IS A CLOSED SET, AND `*)` IS A FAILURE (#3733).
#     THE DEFECT: section 5c's `--fix-claude-auth` dispatch had exactly two arms — the four
#     SERVER-* states that seed, and a `*)` catch-all printing "has nothing to seed in state
#     $CLAUDE_TMUX_V" through `info`. So an EXPLICITLY REQUESTED repair was reported as
#     SUCCESSFUL (no `warn`, therefore `--strict` exits 0) for every state in which SERVER
#     PRESENCE IS UNKNOWN: `UNMEASURED` (an unresolvable tmux identity, no hard-bounded
#     `timeout` capability, a wedged server, an unrecognised tmux failure) and `NO-SERVER`
#     (the live read said "no server" AND the isolated cold-start probe could not run
#     either). "There is nothing to seed" is a POSITIVE STATEMENT, and it was being derived
#     from the ABSENCE of a measurement on a TWELVE-state signal where every unmeasured
#     state inherited the permissive branch — this repo's standing shape, one dispatch over.
#     WHY THIS IS A CLASS FIX AND NOT A FOURTH PATCH: it is the FOURTH instance of one family
#     on this branch (a discarded pipeline status; a half-done seed; a repair that could not
#     be ATTEMPTED, section 42; and now a repair reported as nothing-to-do on a
#     non-measurement). Every previous round fixed the CITED SITE, which is exactly why there
#     was a fourth. So the dispatch is now exhaustive by construction, each arm STATES what
#     established its disposition, and `*)` warns instead of excusing.
#     THE TOKEN SET IS DERIVED FROM COMMITTED SOURCE AT RUN TIME, never copied out of prose:
#     a hand-kept list in a test is the same curation the gate's feature lanes exist to
#     avoid, and CLAUDE.md's own token list is prose that decays. The derivation reads the
#     two producer functions in the capability script and collects every literal they can
#     assign to the verdict out-variable.
#     SCOPE, STATED SO IT IS NOT READ AS MORE: the subject is the REPAIR dispatch alone. The
#     two other `case "$CLAUDE_TMUX_V"` blocks in 5c — the `--yes` decline and the
#     observation-line REMEDY table — are deliberately NOT exhaustive and must not be: the
#     remedy table omits SERVER-CARRIES-BOTH and COLD-START-DELIVERS-BOTH because a healthy
#     state has no remedy, and neither block makes a claim about a requested action, so an
#     unnamed state there prints nothing rather than something false. An exhaustiveness
#     assert over them would red on correct input.
# =====================================================================================
# derive_tmux_tokens <capability-file> -> the verdict tokens, one per line, sorted.
# FAIL-CLOSED BY CONSTRUCTION: it prints nothing and returns non-zero if either producer
# function cannot be located, so a refactor that renames or reflows them is a FAIL naming
# the derivation rather than a silent fallback to an empty set — which would excuse every
# token at once, the vacuous green this whole section exists to prevent.
derive_tmux_tokens() {
  local cf="$1" fn found=0
  for fn in claude_tmux_env_verdict_into__untraced claude_tmux_cold_verdict_into; do
    grep -qE "^${fn}\(\) \{" "$cf" || return 1
    found=$((found + 1))
  done
  [ "$found" -eq 2 ] || return 1
  # ONE PASS, BODY-SCOPED: `^<name>() {` opens a body and the next column-zero `}` closes
  # it, which is this file's layout for every function in it. Tokens are read only from
  # inside those two bodies, so `claude_auth_verdict_into`'s OWN states (PROBE-ANSWERED and
  # friends — a different line, a different set) cannot leak in.
  awk '
    /^claude_tmux_env_verdict_into__untraced\(\) \{/ { inb = 1; next }
    /^claude_tmux_cold_verdict_into\(\) \{/          { inb = 1; next }
    inb && /^\}/                                     { inb = 0; next }
    inb                                              { print }
  ' "$cf" \
    | grep -oE "claude_auth_set_var \"\\\$__ov\" '[A-Z][A-Z-]*'" \
    | sed "s/.*'\\(.*\\)'/\\1/" \
    | sort -u
}
# dispatch_arm_labels <bootstrap-file> -> the arm labels of the REPAIR dispatch, one per
# line. Located STRUCTURALLY: the `case "$CLAUDE_TMUX_V" in` whose enclosing `if` tests
# FIX_CLAUDE_AUTH, i.e. the first such `case` following that condition with only comments
# and blanks between them. Returns non-zero if that shape is not found.
dispatch_arm_labels() {
  awk '
    /^[[:space:]]*if \[ "\$FIX_CLAUDE_AUTH" = 1 \]; then$/ { armed = 1; next }
    armed && /^[[:space:]]*(#|$)/                          { next }
    armed && /^[[:space:]]*case "\$CLAUDE_TMUX_V" in$/     { inc = 1; armed = 0; next }
    armed                                                  { armed = 0 }
    inc && /^[[:space:]]*esac$/                            { inc = 0; exit }
    # An arm label is a line of alternation words and a closing paren, nothing else.
    # Comment lines are excluded first: one of them legitimately ends in `)`.
    inc && /^[[:space:]]*#/                                { next }
    inc && /^[[:space:]]*[A-Z*][A-Z*|-]*\)[[:space:]]*$/ {
      gsub(/^[[:space:]]*|\)[[:space:]]*$/, "")
      n = split($0, parts, "|")
      for (i = 1; i <= n; i++) print parts[i]
    }
  ' "$1"
}
# unhandled_tokens <capability-file> <bootstrap-file>: derived tokens named by no arm.
unhandled_tokens() {
  local toks labels t out=''
  toks=$(derive_tmux_tokens "$1") || return 1
  [ -n "$toks" ] || return 1
  labels=$(dispatch_arm_labels "$2") || return 1
  [ -n "$labels" ] || return 1
  while IFS= read -r t; do
    [ -n "$t" ] || continue
    grep -qxF "$t" <<<"$labels" || out="$out$t "
  done <<<"$toks"
  printf '%s' "$out"
}
tok46=$(derive_tmux_tokens "$CAPLIB") || tok46=''
lab46=$(dispatch_arm_labels "$BOOTSTRAP") || lab46=''
tok46n=$(printf '%s\n' "$tok46" | grep -c . || true)
# (a) THE DERIVATION MUST HAVE WORKED. A SANITY CHECK ON THE DERIVATION, not a curated
#     expectation of the whole set: the four SEED states are named in the dispatch itself,
#     so a derivation that cannot find them is broken however many other names it produced.
if [ "$tok46n" -gt 0 ] \
   && grep -qxF SERVER-MISSING <<<"$tok46" && grep -qxF SERVER-STALE <<<"$tok46" \
   && grep -qxF SERVER-INCOMPLETE <<<"$tok46" && grep -qxF SERVER-CONFIG-STALE <<<"$tok46"; then
  ok "repair dispatch: the verdict token set DERIVES from the capability script ($tok46n tokens)"
else
  bad "repair dispatch: the token derivation FAILED (found $tok46n: $(tr '\n' ' ' <<<"$tok46")) — a derivation that cannot answer must never fall back to an empty set"
fi
# (b) THE DISPATCH ARMS MUST HAVE BEEN LOCATED, and `*)` must be among them: the fix's whole
#     shape is "closed set PLUS a failing default", so an absent default is a regression even
#     if every token happens to be named.
if grep -qxF '*' <<<"$lab46"; then
  ok "repair dispatch: the FIX_CLAUDE_AUTH dispatch was located structurally and carries a default arm"
else
  bad "repair dispatch: no default arm was found in the FIX_CLAUDE_AUTH dispatch (labels: $(tr '\n' ' ' <<<"$lab46")) — an untaught state would fall through silently"
fi
# (c) EXHAUSTIVENESS. Every state the producer can emit is named by an arm.
miss46=$(unhandled_tokens "$CAPLIB" "$BOOTSTRAP") || miss46='DERIVATION-FAILED'
if [ -z "$miss46" ]; then
  ok "repair dispatch: EVERY derived verdict token is named by an arm of the repair dispatch"
else
  bad "repair dispatch: these verdict tokens reach the repair dispatch under no named arm: $miss46 — add an arm with its disposition stated, do not rely on the default"
fi
# (d) THE POSITIVE CONTROL. An exhaustiveness assert that matches nothing greens the file
#     vacuously, so it is shown FINDING a real gap: a synthetic state is planted in a COPY of
#     the capability script and the assert must both DERIVE it and NAME it as unhandled. A
#     bare "the assert red" is not evidence either — an unrelated derivation failure produces
#     the same non-empty result — so the planted name itself must appear.
#     THE COPY IS SCANNED, NEVER SOURCED OR RUN, which is why the planted line may sit above
#     the function's own `local` declarations: the subject is what the DERIVATION can see in
#     committed text, and a control that had to be executable would constrain where a real
#     new state is allowed to appear.
#     RED-VERIFIED AGAINST THE REAL DEFECT TOO, out of band and recorded here because the
#     case cannot carry a second copy of bootstrap: run against the PRE-FIX
#     scripts/bootstrap-agent-machine.sh (47417d2be, the two-arm dispatch) this assert reports
#     8 unhandled tokens including both cited ones (UNMEASURED, NO-SERVER); post-fix, none.
ctl46="$tmp/cap-ctl46.sh"
awk '
  { print }
  /^claude_tmux_env_verdict_into__untraced\(\) \{/ {
    print "  claude_auth_set_var \"$__ov\" '\''SERVER-SYNTHETIC-CONTROL'\''"
  }
' "$CAPLIB" >"$ctl46"
ctl46toks=$(derive_tmux_tokens "$ctl46") || ctl46toks=''
ctl46miss=$(unhandled_tokens "$ctl46" "$BOOTSTRAP") || ctl46miss=''
if grep -qxF SERVER-SYNTHETIC-CONTROL <<<"$ctl46toks" \
   && grep -q 'SERVER-SYNTHETIC-CONTROL' <<<"$ctl46miss"; then
  ok "repair dispatch: the exhaustiveness assert FINDS a synthetic new producer state (positive control)"
else
  bad "repair dispatch: a planted new verdict state was NOT reported unhandled (derived=$(tr '\n' ' ' <<<"$ctl46toks") unhandled=$ctl46miss) — the assert would green over a real gap"
fi
# ...and the control must be the ONLY thing it reports, or it is not attributable: a copy
# that differs from the real file in one planted line must otherwise be exhaustively handled.
if [ "$(printf '%s\n' $ctl46miss | grep -c .)" -eq 1 ]; then
  ok "repair dispatch: the control isolates exactly the planted state (and the real file is otherwise complete)"
else
  bad "repair dispatch: the control reported more than the planted state: $ctl46miss"
fi

# ---- BEHAVIOURAL: the three dispositions, through bootstrap's own reporter -------------
# THE STATUS IS ASSERTED BY WARN-COUNT DELTA, section 42's idiom and for its reason: in
# bootstrap `warn()` IS the mechanism `--strict` reads (it increments the counter `--strict`
# exits 1 on), while a bare `--strict` exit code is not attributable here — this sandbox has
# ambient warnings (an origin-less root, the push-probe opt-out) that red it either way. The
# delta is taken over the SAME sandbox and the SAME fixture, with and without
# `--fix-claude-auth`, so the only difference is the requested repair.
bs46_warns() { bs_marked "$1" 'ok|warn' '.' 2>/dev/null | grep -c '\[warn\]' || true; }
# fixture, expected verdict, expected warn delta, what established the disposition
for spec46 in \
  'broken:UNMEASURED:1:the live read produced no answer at all' \
  'probefail:NO-SERVER:1:the cold-start probe could not run either' \
  'no-server:COLD-START-DELIVERS-BOTH:0:no server is running, MEASURED by the cold probe' \
  'complete:SERVER-CARRIES-BOTH:0:the running server already carries both values'
do
  m46=${spec46%%:*}; r46=${spec46#*:}; v46=${r46%%:*}; r46=${r46#*:}
  dl46=${r46%%:*}; why46=${r46#*:}
  d46=$(mkshim "$tmp/s46-$m46"); plant_bootstrap_quiet_stubs "$d46"
  plant_claude_probe_env "$d46"; plant_tmux "$d46" "$m46"
  bs46fix=$(run_bootstrap_in "$d46" "$ef2" --skip-smoke --skip-push-probe --fix-claude-auth)
  bs46no=$(run_bootstrap_in "$d46" "$ef2" --skip-smoke --skip-push-probe)
  printf '%s\n%s\n' "$bs46fix" "$bs46no" >>"$TRANSCRIPT"
  # THE FIXTURE MUST HAVE REACHED THE STATE, asserted first: a case whose verdict is not the
  # one it was written for tests a different arm and passes for the wrong reason.
  if grep -q "claude-tmux-env: $v46\$\|claude-tmux-env: $v46 " <<<"$bs46fix"; then
    ok "repair dispatch [$m46]: the fixture really produces $v46"
  else
    bad "repair dispatch [$m46]: the fixture did not produce $v46, so this case tests nothing: $(printf '%s' "$bs46fix" | grep 'claude-tmux-env:')"
  fi
  w46f=$(bs46_warns "$bs46fix"); w46n=$(bs46_warns "$bs46no")
  if [ "$w46f" -eq "$((w46n + dl46))" ]; then
    if [ "$dl46" -eq 1 ]; then
      ok "repair dispatch [$m46]: an unmeasurable server state adds EXACTLY ONE [warn] ($w46f vs $w46n) — --strict reds on a repair that neither happened nor was shown unnecessary"
    else
      ok "repair dispatch [$m46]: nothing-to-seed is affirmatively established ($why46), so it adds NO [warn] — --strict cannot red on it"
    fi
  else
    bad "repair dispatch [$m46]: warn delta is $((w46f - w46n)), expected $dl46 (requested=$w46f not-requested=$w46n) — the status does not follow the disposition"
  fi
  # THE LINE MUST SAY WHICH OF THE TWO THINGS HAPPENED. A `warn` that reds without saying the
  # repair did not happen sends an operator looking for a health problem, and an `info` that
  # says "nothing to seed" without saying what established it is the defect itself.
  if [ "$dl46" -eq 1 ]; then
    if bs_marked "$bs46fix" warn 'claude-auth-repair:' >/dev/null \
       && grep -qi 'CANNOT say it was unnecessary' <<<"$bs46fix"; then
      ok "repair dispatch [$m46]: the [warn] states BOTH facts — not performed, and not shown unnecessary"
    else
      bad "repair dispatch [$m46]: the unmeasurable state produced no attributable claude-auth-repair [warn]: $(printf '%s' "$bs46fix" | grep 'claude-auth-repair:' | head -3)"
    fi
  else
    if grep -q 'claude-auth-repair:.*nothing' <<<"$bs46fix" \
       && ! bs_marked "$bs46fix" warn 'claude-auth-repair:' >/dev/null; then
      ok "repair dispatch [$m46]: nothing-to-seed is REPORTED and carries no [warn]"
    else
      bad "repair dispatch [$m46]: a state with nothing to seed reported nothing, or warned: $(printf '%s' "$bs46fix" | grep 'claude-auth-repair:' | head -3)"
    fi
  fi
  # AND A REQUESTED REPAIR MUST NOT HAVE TOUCHED THE SERVER in any of these four states:
  # `tmux setenv -g` needs a running server, and firing it blind on an UNKNOWN state would
  # print a failure that says nothing about the box while overwriting whatever a server that
  # DOES exist happens to hold.
  if [ ! -f "$d46/tmux-calls.log" ] || ! grep -q '^setenv ' "$d46/tmux-calls.log"; then
    ok "repair dispatch [$m46]: no seed was attempted (a blind 'setenv -g' would overwrite an unknown state)"
  else
    bad "repair dispatch [$m46]: the run seeded a server in state $v46: $(cat "$d46/tmux-calls.log")"
  fi
done

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
#     THE HANDOVER LINE IS THE FIRST `chown`/`chmod` IN THE BODY, not a fixed spelling. The
#     handover was `if ! chown -R …` and is now a chain of narrower `chmod`/`chown` grants
#     (#3733), so a pattern anchored on the old spelling stopped matching — which the
#     fail-closed branch below correctly reported rather than passing.
# cp_handover_ln <body>: ONE implementation, called by both the real scan and its positive
# control. Two rounds ago these were separate greps and the control silently stopped finding
# its plant when the real pattern changed — a positive control that drifts from the thing it
# validates is worse than none.
cp_handover_ln() {
  printf '%s\n' "$1" | grep -nE '^[[:space:]]*(if ! )?(chown|chmod) |^[[:space:]]*\|\| ! (chown|chmod) ' | head -1 | cut -d: -f1
}
cp_chown_ln=$(cp_handover_ln "$cp_body")
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
  cp_planted_ln=$(cp_handover_ln "$cp_planted")
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
# 47. AN ACCUSATION MUST BE ATTRIBUTABLE: `FAILED` IS UNREACHABLE WHILE AN ALTERNATE
#     CREDENTIAL IS PRESENT (#3733, roborev job 433).
#     Section 35 pinned the SUCCESS axis — the probe retains the alternates, so an answer
#     means SOME credential worked and the line must not name the persisted one. The same
#     reasoning was never applied to the FAILURE axis: a rejection means SOME credential was
#     rejected, and `FAILED`'s remedy is "replace the value persisted in /etc/environment".
#     So an invalid ALTERNATE earned a confident instruction to destroy a VALID token —
#     which is this issue's own recorded harm (a rate limit telling the operator to throw a
#     working credential away) surviving on the axis the demotion did not sweep.
#     THE FIX IS THE VERDICT, NOT THE ENVIRONMENT. Scrubbing the alternates would make the
#     accusation attributable and is REFUSED at CLAUDE_AUTH_ALT_CRED_KEYS: it changes what
#     the probe authenticates with, which is a behaviour change hiding behind a report. So
#     the rejection becomes UNMEASURED, naming the alternates — the decision order's own
#     safe-tie rule (ambiguity takes the non-accusing answer) applied to one more shape.
#
#     BOTH DIRECTIONS, WITH ONE VARIABLE BETWEEN THEM. "A false-positive fix that also
#     loses the true positive is not a fix" — so 47a and 47b plant the IDENTICAL rejecting
#     stub and the IDENTICAL persisted file, and differ ONLY in whether an alternate
#     credential is in the probe's environment. A one-sided case here would pass equally
#     well against a library that had deleted the FAILED verdict outright.
# =====================================================================================
# The stub rejects UNCONDITIONALLY — it does not consult the alternate. That is deliberate
# and it is what makes the pair a one-variable contrast: the probe's OUTPUT is identical in
# both runs, so any difference in the verdict is the library's classification and nothing
# else. (Section 35's stub, which answers only WITH the alternate, covers the other shape.)
REJECT_TEXT='API Error: 401 {"type":"error","error":{"type":"authentication_error","message":"invalid x-api-key"}}'
ALT_VALUE='sk-ant-alternate-value-3733-do-not-print'

d47a=$(mkshim "$tmp/s47a"); plant_claude "$d47a" 1 "$REJECT_TEXT"
run_cap "$d47a" "$ef2" "ANTHROPIC_API_KEY=$ALT_VALUE" -- --auth
# THE VERDICT IS THE TOKEN, NOT THE LINE — and the first draft of this case got that
# wrong, which is worth keeping as a note. It required the string `FAILED` to be ABSENT
# from the whole line, and red on correct output: the detail legitimately EXPLAINS that
# FAILED's remedy is what is being withheld. A `grep -q FAILED` over a line whose job is
# to discuss FAILED is a guard that reds on correct input. So the state is read as the
# second FIELD and compared EXACTLY — which also closes the prefix hole a `^claude-auth:
# UNMEASURED` match would leave (an `UNMEASURED-ish` rename would satisfy it).
auth_state_of() { printf '%s' "$1" | awk '/^claude-auth:/ { print $2; exit }'; }
a47=$(printf '%s' "$out" | grep '^claude-auth:')
if [ "$(auth_state_of "$a47")" = UNMEASURED ]; then
  ok "attributable accusation: a rejection with an alternate credential present is UNMEASURED"
else
  bad "attributable accusation: expected state UNMEASURED with ANTHROPIC_API_KEY set, got '$(auth_state_of "$a47")': $a47"
fi
# AND THE REJECTION MUST STILL BE REPORTED. Withholding the accusation must not throw away
# the observation: an UNMEASURED whose text said only "nothing was learned" would send the
# operator looking for an outage that did not happen. The detail has to say a rejection was
# seen AND that it could not be attributed.
if printf '%s' "$a47" | grep -qi 'reject'; then
  ok "attributable accusation: ...and the detail still reports that a rejection was observed"
else
  bad "attributable accusation: the rejection observation was discarded along with the accusation: $a47"
fi
# THE OPERATOR HAS TO BE ABLE TO ACT: an UNMEASURED that does not say WHICH alternate was
# found is a dead end, and naming it is the only route from this line to a re-run.
if printf '%s' "$a47" | grep -q 'ANTHROPIC_API_KEY'; then
  ok "attributable accusation: the line NAMES the alternate credential that made the rejection unattributable"
else
  bad "attributable accusation: the line does not name the alternate, so nothing tells the operator what to unset: $a47"
fi
# NAMES ONLY. The alternate's VALUE is not the persisted secret, so `claude_auth_redact` —
# which is armed with the PERSISTED token — would not mask it: this is the one credential in
# the run that the redaction boundary cannot catch, which is exactly why it is asserted here
# rather than left to the suite-wide sweep in section 23 (that sweep greps for $TOK).
if printf '%s' "$out" | grep -qF -- "$ALT_VALUE"; then
  bad "attributable accusation: the alternate credential's VALUE was printed — names only"
else
  ok "attributable accusation: the alternate's NAME is reported and its VALUE never is"
fi
if printf '%s' "$out" | grep -qF -- "$TOK"; then
  bad "attributable accusation: the persisted token leaked into the unattributable-rejection line"
else
  ok "attributable accusation: ...and the persisted token does not leak on this path either"
fi

# 47b. THE TRUE POSITIVE, PRESERVED. Same stub, same rejection text, same persisted file —
#      no alternate. `run_cap` scrubs all four by default (see the derivation at the top), so
#      this case is clean whatever the invoking shell exports; before that scrub existed, a
#      fleet box with ANTHROPIC_API_KEY set would have made THIS case red and 47a pass for
#      the wrong reason, which is the pair failing as a unit.
d47b=$(mkshim "$tmp/s47b"); plant_claude "$d47b" 1 "$REJECT_TEXT"
run_cap "$d47b" "$ef2" -- --auth
b47=$(printf '%s' "$out" | grep '^claude-auth:')
if [ "$(auth_state_of "$b47")" = FAILED ]; then
  ok "attributable accusation: the SAME rejection with NO alternate present still earns FAILED"
else
  bad "attributable accusation: the narrowing removed the FAILED verdict instead of narrowing it, got: $b47"
fi
# The pair must differ, or one of the two runs is not exercising what it claims.
if [ "$a47" != "$b47" ]; then
  ok "attributable accusation: the one-variable contrast really does change the verdict"
else
  bad "attributable accusation: both runs produced the same line, so the contrast measures nothing: $a47"
fi
# AND THE SCRUB ITSELF IS PINNED, in the direction that can go silently wrong. `run_cap`
# must neutralise an inherited alternate, or 47b (and sections 3, 29f-h and 30) would be
# measuring the invoking shell. Asserted by INHERITING one into the harness for one call:
# if the scrub were dropped, this run would take 47a's branch and read UNMEASURED.
d47c=$(mkshim "$tmp/s47c"); plant_claude "$d47c" 1 "$REJECT_TEXT"
if ANTHROPIC_AUTH_TOKEN="$ALT_VALUE" run_cap "$d47c" "$ef2" -- --auth \
   && [ "$(auth_state_of "$(printf '%s' "$out" | grep '^claude-auth:')")" = FAILED ]; then
  ok "attributable accusation: run_cap scrubs an INHERITED alternate, so every other case measures the code and not the shell"
else
  bad "attributable accusation: an inherited ANTHROPIC_AUTH_TOKEN reached the probe — every FAILED case in this suite is then host-dependent: $(printf '%s' "$out" | grep '^claude-auth:')"
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
# 48. THE LIVE `show-environment -g` READ IS BOUNDED IN BYTES, NOT ONLY IN TIME
#     (#3733, roborev job 440).
#     THE DEFECT. `__out=$(tmux show-environment -g)` accumulates until the producer
#     stops. The op bound bounds TIME; nothing bounded MEMORY. `show-environment -g`
#     prints the SERVER's global table, and on this fleet every lane runs as ONE user
#     while bootstrap is documented to run under `sudo` — so a PEER LANE can `setenv -g`
#     arbitrarily much into the server a ROOT-run report then reads. A non-invoker route,
#     therefore a defect and not a documented limitation.
#     THE CLASS, NOT THE CITED LINE. The finding named the stdout capture; the region
#     held THREE unbounded reads of that one producer — the stdout capture, an unbounded
#     `cat` of the stderr file, and a SECOND unbounded capture in the no-temp-file
#     fallback (now removed: a read with nowhere to put the bytes cannot be capped, and
#     this file's own rule is that an unboundable call is not made).
#     AND THE ESTABLISHED IDIOM WAS ITSELF BROKEN, which is why the read is now ONE
#     SHARED helper rather than a third copy: `$( )` STRIPS EVERY TRAILING NEWLINE, and
#     every producer here is newline-TERMINATED, so a `head -c CAP+1` capture of an
#     over-cap stream came back at exactly CAP and `-gt CAP` was FALSE. Measured on the
#     shipped code before this section existed: an 8 MiB stderr was classified as a tmux
#     failure with a truncated 500-character cause instead of as OVER THE CAP. (c) below
#     is that measurement as a differential.
#     WHY THE CAP IS 4 MiB AND NOT `CLAUDE_AUTH_REPORT_MAX`: section 25 declares a
#     heavily populated table ORDINARY and pins a ~200 KB one and a 5000-variable one as
#     reads that MUST STILL SUCCEED, so a 64 KiB cap would lose the true positive — and
#     the direction it would lose it in is the dangerous one (SERVER-MISSING for a
#     correctly seeded server, whose remedy is to overwrite the value already right).
#     BOTH DIRECTIONS ARE PINNED HERE, and (b) is the one that matters most: an
#     over-cap table must NOT be parsed. The token is planted on the FIRST line, so a
#     truncated-but-accepted parse is directly observable as SERVER-CARRIES-BOTH.
# =====================================================================================
# The cap the library declares. DERIVED, never copied: a cap changed there and hard-coded
# here would leave these cases asserting about a number the code no longer uses.
CAPV=$(sed -n 's/^CLAUDE_AUTH_TMUX_ENV_MAX=\([0-9][0-9]*\)$/\1/p' "$CAPLIB" | tail -1)
case "$CAPV" in ''|*[!0-9]*) CAPV='' ;; esac
if [ -z "$CAPV" ]; then
  bad "byte cap: CLAUDE_AUTH_TMUX_ENV_MAX could not be read from the library, so section 48 cannot assert about it"
  CAPV=4194304
else
  ok "byte cap: the live-read cap is declared in the library and was derived, not copied ($CAPV bytes)"
fi
# A MARKER INSIDE THE PADDING, so "no captured value is echoed" is asserted against
# something identifiable rather than against a length.
PADMARK='CQLITE_PAD_MARKER_MUST_NOT_BE_PRINTED'
ERRMARK='CQLITE_ERR_MARKER_MUST_NOT_BE_PRINTED'
# plant_tmux_sized <dir> <pad-bytes>: a server whose global table carries BOTH keys — the
# token FIRST — followed by <pad-bytes> of padding and a TRAILING NEWLINE. The newline is
# the whole point: it is what the pre-fix capture silently dropped.
plant_tmux_sized() {
  local d="$1" pad="$2"
  rm -f "$d/tmux"
  cat >"$d/tmux" <<EOF
#!/usr/bin/env bash
while [ "\$#" -gt 0 ]; do case "\$1" in -L|-S) shift 2 ;; *) break ;; esac; done
case "\$1" in
  show-environment)
    printf 'CLAUDE_CODE_OAUTH_TOKEN=%s\n' '$TOK'
    printf 'CLAUDE_CONFIG_DIR=%s\n' '$CFGDIR'
    printf 'PAD_%s=' '$PADMARK'
    printf '%*s' '$pad' ''
    printf '\n'
    exit 0 ;;
esac
exit 0
EOF
  chmod +x "$d/tmux"
}

# (a) OVER THE CAP: UNMEASURED, naming the cap.
d48a=$(mkshim "$tmp/s48a"); plant_tmux_sized "$d48a" "$((CAPV + 4096))"
run_cap "$d48a" "$ef2" -- --tmux-env
if printf '%s' "$out" | grep -q '^claude-tmux-env: UNMEASURED' \
   && printf '%s' "$out" | grep -qF -- "$CAPV-byte cap"; then
  ok "byte cap: an over-cap global environment is UNMEASURED and the detail names the cap"
else
  bad "byte cap: an over-cap global environment did not report UNMEASURED naming the cap: $out"
fi
# (b) ...AND IT IS NEITHER AN ACCUSATION NOR A TRUNCATED PARSE. `FAILED` is the accusation
# whose remedy destroys the persisted value, and an over-cap read establishes nothing about
# the credential. SERVER-CARRIES-BOTH here would mean the truncated prefix WAS parsed — the
# token is on line 1, so that state is reachable and would pass unnoticed without this.
if printf '%s' "$out" | grep -qE '^claude-(auth|tmux-env): (FAILED|SERVER-)'; then
  bad "byte cap: an over-cap read produced an accusation or a truncated-but-accepted parse: $out"
else
  ok "byte cap: an over-cap read is never FAILED and never a SERVER-* verdict (no truncated parse)"
fi
# (c) NO CAPTURED VALUE IS ECHOED. The table is peer-controlled text and one of its entries
# is the credential; the detail must carry the cap and nothing from the table.
if printf '%s' "$out" | grep -qF -- "$PADMARK"; then
  bad "byte cap: the over-cap detail echoed content from the server's table"
else
  ok "byte cap: no content from the over-cap table appears in the output"
fi
# (d) UNDER THE CAP: THE TRUE POSITIVE SURVIVES. A fix that loses it is not a fix, and this
# is the direction section 25 exists for.
d48b=$(mkshim "$tmp/s48b"); plant_tmux_sized "$d48b" "$((CAPV - 65536))"
run_cap "$d48b" "$ef2" -- --tmux-env
if printf '%s' "$out" | grep -q '^claude-tmux-env: SERVER-CARRIES-BOTH'; then
  ok "byte cap: a just-under-cap global environment is still read correctly (true positive kept)"
else
  bad "byte cap: a just-under-cap environment was misread: $out"
fi
# (e) THE STDERR HALF, which the finding named alongside the output. tmux FAILS with an
# over-cap cause: the cause cannot be classified from a truncated fragment, so the verdict
# is UNMEASURED naming the cap — not "failed for a reason that is not a missing server"
# with 500 characters of someone else's text pasted after it.
d48c=$(mkshim "$tmp/s48c")
cat >"$d48c/tmux" <<EOF
#!/usr/bin/env bash
while [ "\$#" -gt 0 ]; do case "\$1" in -L|-S) shift 2 ;; *) break ;; esac; done
printf 'ERR_%s=' '$ERRMARK' >&2
printf '%*s' '$((CAPV + 4096))' '' >&2
printf '\n' >&2
exit 1
EOF
chmod +x "$d48c/tmux"
run_cap "$d48c" "$ef2" -- --tmux-env
if printf '%s' "$out" | grep -q '^claude-tmux-env: UNMEASURED' \
   && printf '%s' "$out" | grep -qF -- "$CAPV-byte cap" \
   && ! printf '%s' "$out" | grep -qF -- "$ERRMARK"; then
  ok "byte cap: an over-cap tmux STDERR is UNMEASURED naming the cap, with none of it echoed"
else
  bad "byte cap: an over-cap tmux stderr was not reported by its cap: $out"
fi
# (f) THE WRITE IS BOUNDED TOO, or the fix only moves the accumulation from memory to disk.
# An endlessly-writing server must not fill TMPDIR, must not leave a core file, must not
# leave its private directory behind, and must still report the cap. The stub writes
# NUL-FREE output on purpose: a NUL cannot reach a real tmux table (`setenv -g KEY VALUE`
# passes the value through argv), and planting one would only measure bash's own
# null-byte warning.
d48d=$(mkshim "$tmp/s48d")
cat >"$d48d/tmux" <<'EOF'
#!/usr/bin/env bash
while [ "$#" -gt 0 ]; do case "$1" in -L|-S) shift 2 ;; *) break ;; esac; done
case "$1" in show-environment) exec yes 'PAD_ENDLESS=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' ;; esac
exit 0
EOF
chmod +x "$d48d/tmux"
wb_tmp="$tmp/writebound-tmp"; mkdir -p "$wb_tmp"
wb_t0=$SECONDS
run_cap "$d48d" "$ef2" TMPDIR="$wb_tmp" -- --tmux-env
wb_dt=$((SECONDS - wb_t0))
wb_left=$(glob_matches "$wb_tmp/cqlite-tmuxenv.*")
wb_bytes=$(find "$wb_tmp" -type f -exec cat {} + 2>/dev/null | wc -c)
if printf '%s' "$out" | grep -qF -- "$CAPV-byte cap" && [ "$wb_dt" -le 60 ] \
   && [ -z "$wb_left" ] && [ "$wb_bytes" -eq 0 ]; then
  ok "byte cap: an endlessly-writing server is reported by its cap in ${wb_dt}s, leaving no bytes and no directory"
else
  bad "byte cap: an endlessly-writing server was not bounded on the WRITE side (dt=${wb_dt}s left='$wb_left' bytes=$wb_bytes): $out"
fi
if [ -z "$(glob_matches "$wb_tmp/core*")" ] && [ -z "$(glob_matches "$tmp/core*")" ]; then
  ok "byte cap: hitting the write bound leaves no core file (SIGXFSZ is ignored, not taken)"
else
  bad "byte cap: hitting the write bound dumped core"
fi

# --- (g) POSITIVE CONTROL 1: THE UNBOUNDED, UNCAPPED READ ---------------------------
# A case that asserts "an over-cap table is refused" proves nothing until it is shown to
# RED against the read that did not refuse it. So the fix is reverted in a THROWAWAY COPY
# of the library — the shared capped read becomes the pre-fix unbounded `cat` — and (a)/(b)
# are re-run against it. THE PATCH IS ASSERTED TO HAVE TAKEN: a control whose plant silently
# failed is a case that passes for the wrong reason.
# run_cap_lib <lib> <shimdir> <envfile> [pre...] -- args: `run_cap` against a NAMED library.
run_cap_lib() {
  local lib="$1" shimdir="$2" envfile="$3"; shift 3
  local -a pre=()
  while [ "$#" -gt 0 ] && [ "$1" != '--' ]; do pre+=("$1"); shift; done
  [ "${1:-}" = '--' ] && shift
  rc=0
  out=$(PATH="$shimdir:$PATH" env -u SUDO_USER -u SUDO_UID -u CLAUDE_CONFIG_DIR \
        "${alt_scrub[@]}" \
        CQLITE_BOOTSTRAP_TEST_MODE=1 \
        CQLITE_CLAUDE_AUTH_ENV_FILE="$envfile" \
        ${pre[@]+"${pre[@]}"} \
        bash "$lib" "$@" 2>&1) || rc=$?
  printf '%s\n' "$out" >>"$TRANSCRIPT"
}
ctl48="$tmp/ctl48"; mkdir -p "$ctl48"
ctl48lib="$ctl48/claude-auth-capability.sh"
# BOTH HALVES OF THE PROTECTION ARE REVERTED, because reverting one is not the pre-fix
# state and the first draft of this control proved it: with the READ made unbounded but the
# oversize VERDICT left in place, `__olen` simply became the FULL length and the copy still
# refused — a control that passed the case it was meant to red. Pre-fix there was no cap and
# no refusal, so the copy gets neither: the read reads to EOF and the size branch can never
# fire (spelled as an impossible comparison, so it stays a size test and cannot be mistaken
# for a deleted line).
sed -e 's|^  __crt_raw=\$(claude_auth_bounded.*$|  __crt_raw=$(cat "$__crt_f" 2>/dev/null)|' \
    -e 's|^  __crt_rc=\${__crt_raw##\*R}$|  __crt_rc=$?|' \
    -e 's|^  __crt_raw=\${__crt_raw%R\*}$|  :|' \
    -e 's|^  if \[ "\$__olen" -gt "\$CLAUDE_AUTH_TMUX_ENV_MAX" \]; then$|  if [ "$__olen" -lt 0 ]; then|' \
    "$CAPLIB" >"$ctl48lib"
if grep -q '__crt_raw=\$(cat "\$__crt_f"' "$ctl48lib" \
   && ! grep -q '^  __crt_raw=\$(claude_auth_bounded' "$ctl48lib" \
   && grep -q '^  if \[ "\$__olen" -lt 0 \]; then$' "$ctl48lib" \
   && ! grep -q '^  if \[ "\$__olen" -gt "\$CLAUDE_AUTH_TMUX_ENV_MAX" \]; then$' "$ctl48lib" \
   && bash -n "$ctl48lib" 2>/dev/null; then
  ok "byte cap control: the pre-fix unbounded read was planted in a throwaway copy (and it parses)"
  run_cap_lib "$ctl48lib" "$d48a" "$ef2" -- --tmux-env
  if printf '%s' "$out" | grep -q '^claude-tmux-env: SERVER-CARRIES-BOTH'; then
    ok "byte cap control: pre-fix, the SAME over-cap table is read whole and reported SERVER-CARRIES-BOTH — so (a) and (b) discriminate"
  else
    bad "byte cap control: the pre-fix copy did not reproduce the unrefused read, so (a)/(b) prove nothing: $out"
  fi
else
  bad "byte cap control: the pre-fix read could not be planted, so (a)/(b) have no positive control"
fi

# --- (h) POSITIVE CONTROL 2: THE TRAILING-NEWLINE UNDER-MEASUREMENT -----------------
# The half a behavioural case cannot show, because both idioms REFUSE an over-cap stream by
# some route: the pre-fix capture MIS-MEASURES it. One file, two reads, same cap — the old
# idiom (`$( )` then `${#…}`) must land exactly AT the cap while the shipped helper reports
# CAP+1. This is the shipped defect reduced to two numbers.
# THE FIXTURE PUTS A NEWLINE AT EXACTLY BYTE CAP+1, which is the condition the defect
# needs and is not a contrivance: for a table of short lines the CAP+1st byte is a newline
# roughly once per line length, and a stream of short lines is exactly what an enlarged
# environment is. (Padding that is one enormous line does NOT reproduce it — the first draft
# of this control used one and measured CAP+1 from both idioms, because the only newline was
# far past the cut. That is worth knowing: the defect is intermittent by content, which is
# why it survived review.) The file is longer than CAP+1, so it is genuinely over the cap.
nl_dir="$tmp/newline-cap"; mkdir -p "$nl_dir"
nl_f="$nl_dir/stream"
{ printf '%*s' "$CAPV" ''; printf '\nover\n'; } >"$nl_f"
nl_old=$(bash -c 'v=$(head -c "$(( $2 + 1 ))" "$1" 2>/dev/null); printf "%s" "${#v}"' _ "$nl_f" "$CAPV")
nl_new=$( (
  # SOURCED IN A SUBSHELL so none of the library's globals reach the cases; its `main` runs
  # only under its own `BASH_SOURCE[0] = $0` guard.
  # shellcheck source=/dev/null
  . "$CAPLIB" >/dev/null 2>&1
  claude_auth_capped_read_into nl_t nl_r nl_b 10 "$CAPV" "$nl_f"
  printf '%s' "$nl_b"
) )
if [ "$nl_old" = "$CAPV" ] && [ "$nl_new" = "$((CAPV + 1))" ]; then
  ok "byte cap control: the pre-fix capture under-measures a newline-terminated over-cap stream ($nl_old = the cap) where the shared read reports $nl_new"
else
  bad "byte cap control: the trailing-newline differential did not reproduce (old=$nl_old new=$nl_new cap=$CAPV)"
fi

# --- (i) STRUCTURAL: NO UNBOUNDED READ OF THESE STREAMS MAY COME BACK ---------------
# Behavioural cases cover the shapes someone thought of. This one asserts the CLASS: the
# library captures no command output with `$(cat …)` and never captures a tmux invocation
# into a variable, and every capped read goes through the ONE shared helper. Whole-line
# comments are blanked first — this section's own prose names both shapes, and a guard that
# reds on the text describing its subject is the guard agents learn to delete.
scan_unbounded_capture() {
  sed -e 's/^[[:space:]]*#.*$//' "$1" | grep -nE '=\$\((cat|claude_auth_tmux_run)[[:space:]]'
}
ctl48b="$ctl48/planted-capture.sh"
{
  printf '%s\n' '#!/usr/bin/env bash'
  printf '%s\n' '# a comment naming $(cat "$f") must NOT red'
  printf '%s\n' 'safe=$(claude_auth_capped_read_into a b c 10 99 "$f")'
  printf '%s\n' 'bad_one=$(cat "$f" 2>/dev/null)'
} >"$ctl48b"
ctl48b_hits=$(scan_unbounded_capture "$ctl48b")
if printf '%s' "$ctl48b_hits" | grep -q 'bad_one' \
   && [ "$(printf '%s\n' "$ctl48b_hits" | grep -c .)" -eq 1 ]; then
  ok "byte cap guard: the unbounded-capture scanner finds a planted \$(cat …) (and only it)"
else
  bad "byte cap guard: the scanner did not isolate the planted unbounded capture: $ctl48b_hits"
fi
cap48_hits=$(scan_unbounded_capture "$CAPLIB")
if [ -z "$cap48_hits" ]; then
  ok "byte cap guard: the library captures no command output through an unbounded \$(cat …) or tmux substitution"
else
  bad "byte cap guard: an unbounded capture is back in the library: $cap48_hits"
fi
cap48_uses=$(grep -c '^[[:space:]]*claude_auth_capped_read_into ' "$CAPLIB")
if [ "$cap48_uses" -eq 3 ]; then
  ok "byte cap guard: all three capped reads (live stdout, live stderr, pane report) go through the ONE shared helper"
else
  bad "byte cap guard: expected 3 call sites of the shared capped read, found $cap48_uses"
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
# round 5's two probe-working-directory interrupt cases, and 124 -> 179 by #3733's
# DEMOTION, the handover fix, the three repair-status fixes, the bounded report read, the
# argument validation, the opt-out-override report, the removal of the substitution
# capability and the removal of eval-based output assignment.
# Sections 34-36 (the
# no-certification invariant, the alternate-credential observation, the
# limitation-findability guard and the live/FIXED split), section 37 (the handover ordering,
# behavioural + structural + its positive control), section 38 (an explicitly requested
# repair that FAILED must red, and a SUCCESSFUL one must not), the seam-refusal exit-status
# pin, the marker-scanner positive control, section 39 (a PARTIAL repair must not report
# success, and a COMPLETE one must still exit 0), and the assertions that changed subject
# where a verdict became an observation, and section 40 (the pane report is read ONCE, under
# the bound — the fifo-substitution BLOCKER) and section 41 (unknown/extra arguments are
# refused with status 2 and never reach the billed probe), section 42 (a requested repair
# that could not be ATTEMPTED is an action failure) and section 43 (opt-out plus
# --fix-claude-auth is already decided both ways, and says which intent lost) and section 44
# (the substitution capability is REMOVED, not detected) and section 45 (output-variable
# assignment never goes through `eval`, with the surviving eval a closed set of one) and
# section 46 (the repair dispatch is a CLOSED SET derived from the producer, `*)` is a
# FAILURE, and the three dispositions are pinned behaviourally by warn-count delta), and
# section 47 (`FAILED` is an ACCUSATION, so it is unreachable while an alternate credential
# makes the rejection unattributable — pinned in BOTH directions off one variable, plus the
# runner-level scrub without which every FAILED case in this suite is host-dependent), and
# section 48 (the live `show-environment -g` read is bounded in BYTES and not only in time —
# both directions, plus the two positive controls that show the pre-fix read is NOT refused
# and the pre-fix capture MIS-MEASURES a newline-terminated over-cap stream).
# 226 cases run, and there are
# TWO legitimately skippable cases, not one: the real-tmux isolation case (3 assertions) and
# section 44's OS-mechanism case, which cannot measure "a non-owner is refused" when the
# suite itself runs as root. The floor therefore excludes both — it is the count that runs on
# EVERY host, and a floor that included a case skippable on a legitimate host is a floor that
# reds on correct input.
# THE FIGURE IS MEASURED, NOT COUNTED BY EYE, AND IT IS RE-MEASURED WHENEVER IT MOVES:
# forcing the tmux block's `command -v tmux` test to `true` AND section 44(a)'s
# root-owned-directory guard to `false` in a throwaway `git worktree` reports 222/0/2 with
# BOTH skippable branches forced (179 -> 200 by section 46's 21 cases: the derivation, the
# structural location, the exhaustiveness assert, its positive control and the control's
# isolation, plus four fixtures x four assertions; 200 -> 208 by section 47's 8 — five on
# the alternate-present rejection, two on the no-alternate contrast, one on the runner
# scrub; 208 -> 222 by section 48's 14 — the derived cap, four assertions on an over-cap
# table, the just-under-cap true positive, the over-cap stderr, the two write-bound ones,
# the three-part unbounded-read positive control, the trailing-newline differential and the
# three structural guards). The value in this file is the authority — a figure quoted in a commit
# message is a snapshot of the run that produced it and does not follow later edits.
CASE_FLOOR=222
if [ "$((PASS + FAIL))" -lt "$CASE_FLOOR" ]; then
  printf 'FAIL - case floor: %s cases ran, expected at least %s (cases were lost)\n' "$((PASS + FAIL))" "$CASE_FLOOR"
  exit 1
fi
[ "$FAIL" -eq 0 ]
