#!/usr/bin/env bash
# shellcheck shell=bash
# claude-auth-capability.sh — THE one place that REPORTS what can be observed about a
# fleet box's ability to start `claude` on a COLD, non-interactive lane (issue #3733).
#
# THIS FILE CERTIFIES NOTHING, AND THAT IS A DESIGN DECISION, NOT AN OMISSION (#3733, lead
# ruling). It used to emit two CERTIFYING verdicts whose passing state was `VERIFIED`.
# Three consecutive independent reviews of that design each found a NEW High-severity
# defect, and all three were ONE shape: THE PROBE CANNOT OBSERVE THE PROPERTY ITS VERDICT
# NAMED. The cold-start probe proves tmux PROPAGATION, not pam_env DELIVERY (LIMITATION 1);
# the `claude -p` probe proves that SOMETHING in its environment authenticated, not that
# the PERSISTED value did (LIMITATION 2); `[ -d ]` proves a directory exists TO US, not
# that the delegated agent can use it (LIMITATION 3). Each individual fix was correct and
# the family kept regenerating, which in this repository is the standing signal to change
# the DESIGN rather than carve the same place a fourth time.
#
# So both lines are OBSERVATIONS. They print what was found; they never say the box is
# ready. Concretely, and these are the properties `scripts/tests/test_claude_auth_capability.sh`
# section 34 pins as an INVARIANT rather than case by case:
#   * no state either line emits is named `VERIFIED` — the word a pasted log reads as a
#     certification whatever the surrounding prose says;
#   * every entry point prints a `claude-auth-report: OBSERVATIONS-ONLY` scope note, so an
#     operator reading ONE pasted line learns it certifies nothing;
#   * THE EXIT STATUS CARRIES NO VERDICT. A report that was PRINTED exits 0 whatever it
#     found, so the best- and worst-looking boxes are indistinguishable by exit status and
#     `if script --auth; then …` cannot be written. That is the load-bearing half: a state
#     rename alone would leave an exit-code gate intact, and an exit code is exactly how a
#     caller acts on a proxy.
# The AC1 mechanism knowledge below is the durable value of #3733 and is UNCHANGED — the
# six measured facts are what tell an operator where to look. What was withdrawn is the
# claim that any of them can be VERIFIED from here.
#
# FIVE NUMBERED SLOTS: FOUR LIVE LIMITATIONS AND ONE FIXED, indexed so a reader can find
# each at its own code site (grep `LIMITATION <n> of 5`). The four live ones are limitations
# of a REPORT, not defects — but a reader must never have to rediscover them. Slot 4 is a
# RECORD of one that was filed here, reclassified and FIXED: the numbers are not reused and
# nothing is renumbered, so every reference written while it was live still resolves.
#   1. the COLD-START probe injects the /etc/environment values into its OWN throwaway
#      server, so it observes tmux propagation and NOT pam_env delivery
#      (claude_tmux_cold_probe_into);
#   2. the `claude -p` probe does not neutralise ANTHROPIC_API_KEY, ANTHROPIC_AUTH_TOKEN,
#      CLAUDE_CODE_USE_BEDROCK or CLAUDE_CODE_USE_VERTEX, so an answered probe means SOME
#      credential in its environment worked (claude_auth_verdict_into__untraced);
#   3. `[ -d <config dir> ]` is evaluated as THIS process — root, under the documented sudo
#      invocation — so it says nothing about the delegated agent's access
#      (claude_tmux_env_verdict_into__untraced and claude_tmux_cold_verdict_into);
#   4. FIXED (was: the probe directory's `chown -R` precedes the heredoc that writes
#      `probe.sh`, so that one file is not covered by the handover). RECLASSIFIED first:
#      root writing into a directory it has already given away is not a claim that could be
#      wrong, it is a same-uid PEER LANE's opportunity to interpose a symlink and have ROOT
#      overwrite an arbitrary file — a non-invoker route, so a defect and not a limitation
#      of a report. The write now precedes the handover; the umask half closed with it
#      (claude_tmux_cold_probe_into);
#   5. the LIVE-server path reads the tmux GLOBAL environment only and never spawns a pane
#      (claude_tmux_env_verdict_into__untraced).
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
# HENCE TWO INDEPENDENT OBSERVATION LINES, each greppable, because they observe different
# things and their remedies differ. THE STATE NAMES SAY WHAT WAS SEEN, NOT WHETHER IT IS
# GOOD — that is why the formerly-passing states were RENAMED rather than merely demoted:
#
#   claude-auth:      what happened when a `claude -p` run carrying the persisted value was
#                     made?
#                     PROBE-ANSWERED | NOT-PERSISTED | FAILED | UNMEASURED
#                     PROBE-ANSWERED means rc 0 AND the sentinel came back. It does NOT say
#                     the persisted value is what authenticated (LIMITATION 2).
#                     FAILED is reserved for a POSITIVELY IDENTIFIED rejection; every other
#                     unsuccessful probe (rate limit, outage, quota, crash, no sentinel) is
#                     UNMEASURED with its cause named — see the matcher block below.
#   claude-tmux-env:  what does a tmux server hold, or what would a new one hand a pane?
#                     live server:  SERVER-CARRIES-BOTH | SERVER-STALE | SERVER-MISSING |
#                                   SERVER-INCOMPLETE | SERVER-CONFIG-STALE |
#                                   SERVER-CONFIG-NODIR
#                     no server:    COLD-START-DELIVERS-BOTH | COLD-START-MISSING |
#                                   COLD-START-INCOMPLETE | COLD-START-NODIR
#                     either:       NO-SERVER | UNMEASURED
#                     The live and cold "both present" states are DELIBERATELY DIFFERENT
#                     TOKENS: they are different observations (a server's global table vs a
#                     throwaway server's propagation), and one name for both hid which had
#                     been made.
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
# NONEMPTY IS NOT CORRECT. `*-CARRIES-BOTH`/`*-DELIVERS-BOTH` require the server's
# CLAUDE_CONFIG_DIR to EQUAL the persisted value AND that directory to EXIST — testing only
# "is it absent" and letting every other state fall through to the good one is the
# two-valued predicate that always picks the permissive answer, and a wrong config dir is
# the un-onboarded first-run picker this file exists to describe. TWO DECLARED RESIDUALS:
# `exists` is not `onboarded` (whether a config directory holds usable onboarding state is
# deliberately NOT probed — that would mean depending on an internal JSON field shape that
# can change upstream, and a check that silently stops matching would report the good state
# for the wrong reason); and `exists` is `exists TO THIS PROCESS` (LIMITATION 3).
#
# NO STATE ON EITHER LINE IS A SUCCESS. NO-SERVER is UNMEASURED-class: the isolated
# cold-start probe could not run, so nothing was measured (it does NOT merely mean "no
# server was running" — that case is measured now). An unmeasured capability never inherits
# the permissive branch — the standing rule that a positive verdict requires an AFFIRMATIVE
# MEASUREMENT (docs/development/fleet-runbook.md; CLAUDE.md, "git-push:"/"gate-pin:") — and
# under this ruling the affirmative measurement for "this box can start a lane" is not
# available from here at all, so the line reports and stops.
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
#     overrides it — a PROBE-ANSWERED that is about the inherited value. Pinned by case 2b.
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
# AND THE REDACTION BOUNDARY IS DOWNSTREAM OF SHELL TRACING, so it cannot help there: `bash
# -x` prints an expanded assignment and a command's ARGV BEFORE the command runs. Every
# public entry point therefore suppresses xtrace for its duration and restores the caller's
# setting — see "shell tracing" below. Measured on the code that shipped without it: 8
# occurrences of the token under `bash -x … --auth`, 16 under `--report`.
#
# PLATFORM. /etc/environment + pam_env is Linux-specific. On a non-Linux host BOTH lines
# are UNMEASURED: scoping a platform out is not the same as passing it. (Since the demotion
# no state here is an [ok] anywhere, so this is now about the TEXT being honest rather than
# about `--strict`; the #3414 defect it was written for cannot recur.)
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

# ---- classifying an UNSUCCESSFUL probe: three matchers, one accusation ---------------
# `FAILED` means "the persisted credential was REJECTED", and its remedy is "replace the
# VALUE — bootstrap never rewrites it". That is a confident, actionable instruction about a
# credential, so it must be EARNED by a positively identified rejection. Everything else
# unsuccessful is UNMEASURED with the cause named: the standing rule that where the sole
# oracle could not be consulted the verdict is non-passing and its text says what was
# unverifiable — it must not become an affirmative negative.
#
# Nothing is weakened by that move. UNMEASURED is already non-passing, exits non-zero, and
# withholds "All checks green" under `--strict` exactly as FAILED does; only the operator's
# next action differs, which is the whole point. And it closes a real hazard: a rate limit,
# an outage, a quota error or a CLI crash used to instruct the operator to throw away a
# WORKING token — this issue's own history, a measurement of something adjacent reported as
# the thing itself.
#
# THE DECISION ORDER, DERIVED FROM THAT RULE AND STATED ONCE — the code below follows this
# list and nothing else, so change the list and the code together:
#   1. the probe was KILLED by its own bound (rc 124/137) — we ended it; nothing was learned;
#   2. TRANSPORT: the API was not reached, so it never saw the credential;
#   3. SERVICE: the API was reached and refused for a reason that is NOT about this
#      credential — a rate limit, a 5xx, an overload, an exhausted quota;
#   4. REJECTION: only here, with NO benign explanation present in the output, is FAILED
#      earned;
#   5. rc 0 with no sentinel — it did not answer, which is not evidence of rejection;
#   6. anything else — non-zero with nothing identified.
# THE ORDER *IS* THE SAFE-TIE RULE, not a detail of it: a message naming both a benign
# cause and a rejection is ambiguous, and ambiguity must take the non-accusing answer. Only
# rule 2 was in front of rejection at first, so a 429 body that also carried the words
# `authentication_error` was classified FAILED and the operator was told to replace a
# potentially VALID token — the exact harm the FAILED/UNMEASURED split exists to remove,
# surviving in the tie case. Steps 3 and 4 are therefore in this order for a REASON, not by
# accident of how they were written.
#
# `401`/`429`/`5xx` are anchored on non-digits, because a status code matched inside a
# larger number would earn an accusation from a coincidence.
CLAUDE_AUTH_NETWORK_RE='getaddrinfo|ENOTFOUND|ECONNREFUSED|EAI_AGAIN|network is unreachable|temporary failure in name resolution|connection error'
CLAUDE_AUTH_REJECT_RE='invalid api key|invalid x-api-key|invalid[ _-]?token|invalid credentials|authentication[ _-](error|failed|required)|failed to authenticate|unauthorized|(^|[^0-9])401([^0-9]|$)|oauth (token|session) (has )?expired|could not be refreshed|please run /login|not logged in|api key .{0,24}(invalid|rejected|revoked|expired)|credentials? .{0,24}(invalid|rejected|revoked|expired)'
# THIS MATCHER DECIDES A VERDICT IN THE TIE CASE, and that is a change from what this
# comment used to claim ("only a better SENTENCE for the same UNMEASURED verdict"): because
# it is tested BEFORE the rejection matcher, a shape it recognises keeps a co-occurring
# rejection wording from earning FAILED. Widening it can therefore turn a FAILED into an
# UNMEASURED — which is the safe direction, and the intended one — while a shape it does
# not recognise still lands on the generic UNMEASURED branch below.
CLAUDE_AUTH_SERVICE_RE='rate.?limit|(^|[^0-9])429([^0-9]|$)|too many requests|quota|credit balance|overloaded|(^|[^0-9])5[0-9][0-9]([^0-9]|$)|service unavailable|internal server error|bad gateway'
# The cold-start tmux probe is local-only (no network), so it gets a much tighter bound.
CLAUDE_AUTH_TMUX_PROBE_BOUND=20

# ---- alternate credentials: OBSERVED AND NAMED, DELIBERATELY NOT SCRUBBED ----------
# LIMITATION 2 of 5 (#3733) lives at the probe's `env` call; this is the list it reports.
# `claude` will authenticate from any of these, so a probe that leaves them in place can
# return the sentinel with the persisted value playing no part. They are NOT scrubbed:
# under the lead's ruling this file REPORTS rather than certifies, and silently changing
# what the probe authenticates with would be a behaviour change hiding behind a report.
# What it does instead is NAME the ones present, so a reader of the line knows the probe's
# environment held another way in. NAMES ONLY — never a value; two of these are secrets.
CLAUDE_AUTH_ALT_CRED_KEYS='ANTHROPIC_API_KEY ANTHROPIC_AUTH_TOKEN CLAUDE_CODE_USE_BEDROCK CLAUDE_CODE_USE_VERTEX'
# claude_auth_alt_credentials_into <outvar>: a space-separated list of the alternate
# credential variables SET (and non-empty) in this process's environment, or empty.
# `${!k+set}` distinguishes set-empty from unset; an empty value authenticates nothing, so
# only a non-empty one is reported — reporting a set-empty variable would be a false alarm.
claude_auth_alt_credentials_into() {
  local __o="$1" __k='' __acc=''
  eval "$__o="
  # shellcheck disable=SC2086  # intentional word-split over the space-separated key list
  # `${!__k:-}` distinguishes nothing: it is the VALUE that matters, and a set-but-empty
  # variable authenticates nothing, so only a non-empty one is reported. Reporting an empty
  # one would be a false alarm in a line whose whole job is to be believable.
  for __k in $CLAUDE_AUTH_ALT_CRED_KEYS; do
    [ -n "${!__k:-}" ] || continue
    __acc="${__acc:+$__acc }$__k"
  done
  eval "$__o=\$__acc"
}

claude_auth_test_mode() { [ "${CQLITE_BOOTSTRAP_TEST_MODE:-}" = 1 ]; }
claude_auth_seam_set()  { [ -n "${CQLITE_CLAUDE_AUTH_ENV_FILE:-}" ]; }

# claude_auth_platform_linux: rc 0 iff this host has the pam_env mechanism at all.
claude_auth_platform_linux() { [ "$(uname -s 2>/dev/null)" = Linux ]; }

# ---- bounded execution: THE BOUND MUST ESCALATE, OR THERE IS NO PROBE ---------------
# Resolved by PROBING the candidate, not by trusting its name: GNU coreutils installs its
# timeout as `gtimeout` on macOS, and an older BusyBox `timeout` REJECTS --kill-after.
#
# A SIGTERM-ONLY BOUND IS NOT A BOUND, and keeping one was a shipped defect: the resolver
# used to fall back to a `timeout` that had FAILED the --kill-after probe, so a `claude`
# that ignores SIGTERM ran to its own completion and hung the provisioning entry point.
# MEASURED here with a TERM-ignoring child:
#     timeout 2 <child>                -> rc 124 after THIRTY seconds (the child's own
#                                         lifetime — the "bound" bounded nothing)
#     timeout --kill-after=2 2 <child> -> rc 137 after 4 seconds
# The rc even LOOKS like a timeout in the first case, so nothing downstream could tell.
# CLAUDE.md's gate doctrine takes the opposite line for exactly this shape: where the probe
# cannot be BOUNDED, the probe is not run at all — a missing capability must not inherit
# the permissive branch. So an unsupported escalation flag is now a REFUSAL, whose callers
# report UNMEASURED with the cause named.
#
# TWO SPELLINGS, because they are the same capability under different names: `--kill-after=`
# (GNU long) and `-k` (GNU short AND BusyBox). Each is probed by RUNNING it, so a candidate
# that merely prints the flag in its --help is not taken at its word.
#
# WHAT IS MEASURED AND WHAT IS NOT, declared rather than implied: the resolver measures that
# the escalation flag is ACCEPTED. That the implementation then really escalates to SIGKILL
# is pinned behaviourally, against the real binary and a SIGTERM-ignoring child, by
# scripts/tests/test_claude_auth_capability.sh section 28 — not re-measured on every run,
# because a behavioural probe needs a child that outlives its own bound, i.e. seconds of
# wall clock on a provisioning entry point.
CLAUDE_AUTH_TIMEOUT_BIN=""
CLAUDE_AUTH_TIMEOUT_KILL_ARGS=()
claude_auth_resolve_timeout() {
  local name path
  CLAUDE_AUTH_TIMEOUT_BIN=""; CLAUDE_AUTH_TIMEOUT_KILL_ARGS=()
  for name in timeout gtimeout; do
    path="$(command -v "$name" 2>/dev/null || true)"
    [ -n "$path" ] || continue
    if "$path" --kill-after=1 1 true >/dev/null 2>&1; then
      CLAUDE_AUTH_TIMEOUT_BIN="$path"; CLAUDE_AUTH_TIMEOUT_KILL_ARGS=(--kill-after=5); return 0
    fi
    if "$path" -k 1 1 true >/dev/null 2>&1; then
      CLAUDE_AUTH_TIMEOUT_BIN="$path"; CLAUDE_AUTH_TIMEOUT_KILL_ARGS=(-k 5); return 0
    fi
  done
  CLAUDE_AUTH_TIMEOUT_BIN=""
  return 1
}

# ---- BOUNDING IS A PROPERTY OF EVERY EXTERNAL CALL HERE, NOT OF ONE CALL SITE ------
# The SECOND bounding defect in this file (the first was the `claude -p` probe's
# SIGTERM-only bound), so it is closed as a class. A tmux server that ACCEPTS a connection
# and never answers hangs `show-environment -g` and `setenv -g` forever, and this file's
# primary caller is a provisioning entry point `.agent-ami/profile.yaml` runs UNATTENDED:
# there, an unbounded read is an indefinite hang, not a slow check.
#
# THE CENSUS, so the next call site inherits the decision instead of re-deriving it. Every
# external invocation in this file is either BOUNDED or declared here as unable to block:
#   BOUNDED — `claude -p` (90s, the network probe); every `tmux` invocation, live or
#     throwaway (`show-environment -g`, `setenv -g`, `new-session`, `kill-server`); the
#     pane-report wait loop; the digest tool; and (below) the identity lookups `id` uses.
#     A DELEGATION PREFIX AND A SCRUBBING `env` ARE INSIDE THE BOUND, NOT IN FRONT OF IT:
#     `runuser -u`/`sudo -n -u` and `env -u …` are words of the bounded argv, and `env`
#     EXECVEs its target rather than forking one, so the single process the bound kills is
#     the whole chain — including the `env` that fronts the `claude -p` probe, which is
#     written outside the `timeout` word only because it must scrub the inherited credential
#     before `timeout` is reached.
#   DECLARED UNBOUNDABLE-BY-NEED, each because it cannot block indefinitely:
#     * `uname -s` — a syscall wrapper;
#     * `mktemp`, `rm`, `chown -R` over the probe's own working directory, `cat` reading a
#       file this process wrote and the `cat >` heredoc that writes the pane script into
#       that directory — local filesystem calls on paths WE created, over content of a size
#       this file fixes;
#     * `grep`/`sed`/`tail` over the pam_env file — only reached AFTER `[ -f ]` has
#       established it is a REGULAR file, so there is no fifo or device to block on, and
#       bounding them would make the NOT-PERSISTED verdict itself depend on a `timeout`
#       binary that a correct box need not have (the read must still work when the probe
#       cannot run);
#     * `sed`/`tail` over the pane REPORT file — the ONE read here of a file another process
#       wrote, so it is declared on its own terms rather than riding on the clause above:
#       this process CREATED it (`: >` inside a 0700 `mktemp -d`, so it is a regular file,
#       never a fifo), and the read is reached only after the BOUNDED wait loop has already
#       observed the terminating `end` line in it — so it is complete, regular and small by
#       the time it is read, and a wedged or absent pane is the wait loop's outcome, not a
#       hang here;
#     * `tr`/`cut` inside the redaction boundary, over a string already in memory — a bound
#       that fired there would DESTROY the verdict text it is rendering.
#   NOT AN EXTERNAL CALL AT ALL — `command -v`, `printf` and the `kill -s` signal re-raise
#     are shell builtins, so a missing-binary check, every message and the re-raise spawn
#     nothing.
#   NOT RUN BY THIS PROCESS — the `sh` that executes the pane script runs INSIDE the
#     throwaway tmux server. This process never waits on it: what it waits on is the bounded
#     report loop above, and cleanup kills that server whatever the pane is doing.
# A `timeout` that cannot ENFORCE the bound (no SIGKILL escalation) is not a bound at all;
# `claude_auth_resolve_timeout` refuses such a candidate, and every caller of a bounded
# operation treats that refusal as UNMEASURED rather than running the call unbounded.
CLAUDE_AUTH_TMUX_OP_BOUND=10
CLAUDE_AUTH_DIGEST_BOUND=10
CLAUDE_AUTH_IDENTITY_BOUND=10

# claude_auth_bounded <secs> <cmd> [args...]: run <cmd> under a HARD bound.
# rc is the command's own, or 124/137 when the bound fired, or 125 when NO bound could be
# established (which also happens to be `timeout`'s own "could not run it" code — the two
# are the same operational fact here: the call was not made under a bound, so nothing was
# measured). CALLERS THAT NEED TO TELL A REFUSAL FROM A FAILURE MUST CALL
# `claude_auth_resolve_timeout` THEMSELVES FIRST, because a command substitution runs this
# in a SUBSHELL and any global it sets there is discarded.
claude_auth_bounded() {
  local __secs="$1"; shift
  if [ -n "$CLAUDE_AUTH_TIMEOUT_BIN" ] || claude_auth_resolve_timeout; then
    "$CLAUDE_AUTH_TIMEOUT_BIN" \
      ${CLAUDE_AUTH_TIMEOUT_KILL_ARGS[@]+"${CLAUDE_AUTH_TIMEOUT_KILL_ARGS[@]}"} \
      "$__secs" "$@"
    return $?
  fi
  return 125
}

# claude_auth_bound_fired <rc>: rc 0 iff <rc> says the bound killed the child. 124 is
# `timeout`'s own code; 137 is 128+SIGKILL, i.e. the escalation did the killing.
claude_auth_bound_fired() { [ "${1:-}" = 124 ] || [ "${1:-}" = 137 ]; }

# THE ARGV PREFIX EVERY TMUX CALL CARRIES. Empty when tmux is to be run as this process
# (the ordinary case); an identity delegation when the invoking agent is someone else (see
# the identity block below). It is a GLOBAL because the probe's CLEANUP has to reach the
# same server the probe created, and cleanup runs from a trap with no arguments.
CLAUDE_AUTH_TMUX_PREFIX=()

# claude_auth_tmux_run <secs> <tmux-args...>: one bounded tmux call, as the invoking
# identity. The two LIVE server operations (`show-environment -g`, `setenv -g`) go through
# here; the throwaway-probe calls compose the same two pieces by hand because they also
# interpose the credential-scrubbing `env`.
claude_auth_tmux_run() {
  local __secs="$1"; shift
  claude_auth_bounded "$__secs" \
    ${CLAUDE_AUTH_TMUX_PREFIX[@]+"${CLAUDE_AUTH_TMUX_PREFIX[@]}"} tmux "$@"
}

# ---- WHOSE tmux SERVER? THE INVOKING AGENT'S, NEVER "WHICHEVER UID WE HAPPEN TO BE" ----
# A tmux client with no `-S`/`-L` talks to the DEFAULT SERVER OF THE CURRENT UID. Bootstrap
# prints — and docs/development/fleet-runbook.md documents — `sudo bash
# scripts/bootstrap-agent-machine.sh --yes`, so under the recommended invocation every live
# tmux call addressed ROOT's server while the agent's own, the one that actually spawns
# lanes, stayed exactly as broken as before. Root usually has NO server, so the read failed
# with "error connecting", fell through to the cold-start probe — which measures the
# PERSISTED FILE and passes — and the box was certified VERIFIED while still unable to start
# a lane. That is this issue's own field failure, reintroduced one layer down.
#
# THE IDENTITY IS RESOLVED, AND WHERE IT CANNOT BE, NOTHING IS MEASURED. Three states:
#   self      — no SUDO_USER, or it names the user we already are: tmux runs unwrapped, and
#               the ordinary invocation is byte-for-byte what it was.
#   delegate  — SUDO_USER names someone else and the delegation is constructible: every tmux
#               call is prefixed so it runs AS THAT LOGIN, and therefore against THAT
#               server. TMUX/TMUX_PANE are scrubbed on this path only, because a client
#               started inside a pane connects to the server named in $TMUX and ignores
#               everything else (measured on this repo's own test suite); TMUX_TMPDIR is
#               forwarded when set, since it is the only spelling of "not the default
#               socket directory" available to us.
#   ambiguous — the sudo record is incomplete (only one of SUDO_UID/SUDO_USER), or it
#               contradicts itself, or the uid resolves to no account, or no delegation tool
#               exists. The verdict
#               is UNMEASURED and the repair REFUSES. FALLING BACK TO THE CURRENT UID IS THE
#               PERMISSIVE BRANCH WEARING A DEFAULT'S CLOTHES — it is the very thing that
#               produced the false VERIFIED.
#
# NOT APPLIED TO THE `claude -p` PROBE, deliberately: that one asks whether a CREDENTIAL
# authenticates against the API, using a throwaway config dir. It is a question about a
# token, not about a per-user server, and the answer does not depend on who asks.
CLAUDE_AUTH_TMUX_IDENTITY=''
CLAUDE_AUTH_TMUX_IDENTITY_USER=''
CLAUDE_AUTH_TMUX_IDENTITY_WHY=''

# claude_auth_resolve_tmux_identity: sets the three globals above and CLAUDE_AUTH_TMUX_PREFIX.
# Always rc 0 — the STATE is the answer, and callers must read it rather than an exit code.
claude_auth_resolve_tmux_identity() {
  local __suid="${SUDO_UID:-}" __suser="${SUDO_USER:-}" __who='' __me='' __nuid='' __euid='' __rc=0
  local -a __p=()
  CLAUDE_AUTH_TMUX_IDENTITY=ambiguous
  CLAUDE_AUTH_TMUX_IDENTITY_USER=''
  CLAUDE_AUTH_TMUX_IDENTITY_WHY=''
  CLAUDE_AUTH_TMUX_PREFIX=()

  # NOT UNDER sudo AT ALL: nothing to retarget.
  if [ -z "$__suid" ] && [ -z "$__suser" ]; then
    CLAUDE_AUTH_TMUX_IDENTITY=self
    return 0
  fi
  # SUDO_UID IS THE AUTHORITY AND SUDO_USER MUST AGREE WITH IT. Not a fresh rule: this is
  # the SAME rule bootstrap's gate-pin retarget already follows (#3414 roborev round 8),
  # deliberately reused rather than re-derived, because two implementations of "who invoked
  # me under sudo" in one codebase — with opposite authorities — is a divergence nobody can
  # audit by reading either one. A NAME is subject to NSS mapping and to a shadowed `id`;
  # the UID is what sudo recorded. Sudo sets BOTH, so exactly one of them present is
  # incomplete metadata (stale, hand-exported, or inherited), and incomplete metadata about
  # WHICH SERVER TO ANSWER ABOUT is ambiguity, not a hint.
  if [ -z "$__suid" ] || [ -z "$__suser" ]; then
    CLAUDE_AUTH_TMUX_IDENTITY_WHY='the sudo record is incomplete (one of SUDO_UID/SUDO_USER is set and the other is not), so who invoked this cannot be established — sudo sets both'
    return 0
  fi
  case "$__suid" in ''|*[!0-9]*)
    CLAUDE_AUTH_TMUX_IDENTITY_WHY='SUDO_UID is not numeric, so the invoking agent cannot be identified'
    return 0 ;;
  esac
  # A LOGIN NAME IS VALIDATED BEFORE IT IS USED AS AN ARGUMENT: `id -u -leading-dash` would
  # be read as an option, and this value comes from the environment.
  case "$__suser" in
    *[!A-Za-z0-9._-]*|-*)
      CLAUDE_AUTH_TMUX_IDENTITY_WHY='SUDO_USER is not a plain login name, so the invoking agent cannot be identified'
      return 0 ;;
  esac
  __who=$(claude_auth_bounded "$CLAUDE_AUTH_IDENTITY_BOUND" id -un "$__suid" 2>/dev/null)
  __rc=$?
  if [ "$__rc" -ne 0 ] || [ -z "$__who" ]; then
    CLAUDE_AUTH_TMUX_IDENTITY_WHY="SUDO_UID $__suid does not resolve to an account on this host, so which tmux server belongs to the invoking agent is UNKNOWN"
    return 0
  fi
  __nuid=$(claude_auth_bounded "$CLAUDE_AUTH_IDENTITY_BOUND" id -u "$__suser" 2>/dev/null)
  __rc=$?
  case "$__nuid" in ''|*[!0-9]*) __rc=1 ;; esac
  if [ "$__rc" -ne 0 ] || [ "$__nuid" != "$__suid" ]; then
    CLAUDE_AUTH_TMUX_IDENTITY_WHY="INCONSISTENT sudo metadata — SUDO_USER does not resolve to SUDO_UID $__suid, so which account owns the tmux server is ambiguous and neither answer would be trustworthy"
    return 0
  fi

  # THE NAME USED FROM HERE IS THE ONE RESOLVED FROM THE UID, not the raw SUDO_USER: they
  # have just been proven to agree, and taking the uid's answer keeps the authority in one
  # place.
  __me=$(claude_auth_bounded "$CLAUDE_AUTH_IDENTITY_BOUND" id -un 2>/dev/null)
  __rc=$?
  if [ "$__rc" -ne 0 ] || [ -z "$__me" ]; then
    CLAUDE_AUTH_TMUX_IDENTITY_WHY='this process cannot name its own user (the id -un lookup did not answer), so it cannot tell whether it is already the invoking agent'
    return 0
  fi
  if [ "$__who" = "$__me" ]; then CLAUDE_AUTH_TMUX_IDENTITY=self; return 0; fi

  __euid=$(claude_auth_bounded "$CLAUDE_AUTH_IDENTITY_BOUND" id -u 2>/dev/null)
  case "$__euid" in ''|*[!0-9]*) __euid='' ;; esac
  if [ "$__euid" = 0 ] && command -v runuser >/dev/null 2>&1; then
    __p=(runuser -u "$__who" --)
  elif command -v sudo >/dev/null 2>&1; then
    # `-n`, ALWAYS: a password prompt on a provisioning entry point is an unbounded wait
    # wearing an interactive prompt's clothes.
    __p=(sudo -n -u "$__who" --)
  else
    CLAUDE_AUTH_TMUX_IDENTITY_WHY="the invoking agent is $__who but neither runuser nor sudo is available to run tmux as that login, so only the WRONG server could be reached"
    return 0
  fi
  __p+=(env -u TMUX -u TMUX_PANE)
  [ -z "${TMUX_TMPDIR:-}" ] || __p+=("TMUX_TMPDIR=$TMUX_TMPDIR")
  CLAUDE_AUTH_TMUX_PREFIX=("${__p[@]}")
  CLAUDE_AUTH_TMUX_IDENTITY=delegate
  CLAUDE_AUTH_TMUX_IDENTITY_USER="$__who"
  return 0
}

# ---- identity of a delivered credential: A DIGEST, NEVER A LENGTH ------------------
# The cold-start probe reports what a would-be tmux server DELIVERS to a pane, and the
# delivered token used to be checked against `${#persisted}` alone. LENGTH EQUALITY IS NOT
# VALUE EQUALITY: one `set-environment CLAUDE_CODE_OAUTH_TOKEN <other>` line in a tmux
# config substitutes a different value, and if it is the same length the probe reported
# VERIFIED — which was then the verdict that certified a fresh box as able to start a lane.
# No such verdict exists any more (#3733); the comparison below is why the state it reports
# is worth reporting at all.
#
# COMPARED WITHOUT EVER PRINTING EITHER VALUE, which is this file's standing rule. The pane
# writes a SALTED SHA-256 of what it received; the parent hashes the persisted value with
# the same salt and compares the two hex strings. The salt is generated per run, so what is
# written into the probe's private working directory is not a stable fingerprint of the
# credential either. Neither the digest nor the salt is ever rendered into a verdict.
#
# THE DIGEST TOOL IS A PRECONDITION, NOT A NICETY: with none available the probe REFUSES
# (UNMEASURED-class, cause named) rather than falling back to the length comparison this
# exists to replace. A missing capability must not inherit the permissive branch.
# `sha256sum` (coreutils) and `shasum -a 256` (perl) both print `<hex>  <file>`, so the
# first whitespace-separated field is the digest for either.
CLAUDE_AUTH_DIGEST_CMD=''
claude_auth_resolve_digest() {
  local cand out
  CLAUDE_AUTH_DIGEST_CMD=''
  for cand in 'sha256sum' 'shasum -a 256'; do
    # PROBED BY RUNNING IT, never taken from its name: the same rule as the timeout
    # resolver. A candidate that does not produce 64 hex characters is not a sha256.
    # NOT A PIPE WHOSE rc WE READ: the value is captured, then the SHAPE is tested.
    out=$(printf '%s' cqlite | claude_auth_bounded "$CLAUDE_AUTH_DIGEST_BOUND" $cand 2>/dev/null)
    out=${out%% *}
    case "$out" in
      *[!0-9a-f]*|'') continue ;;
    esac
    [ "${#out}" -eq 64 ] || continue
    CLAUDE_AUTH_DIGEST_CMD="$cand"
    return 0
  done
  return 1
}

# claude_auth_digest_into <outvar> <salt> <value>: rc 1 when no digest could be computed.
# `$CLAUDE_AUTH_DIGEST_CMD` is deliberately UNQUOTED — it is a resolved command WORD LIST
# (`shasum -a 256`), chosen from the closed literal set above and from nowhere else.
claude_auth_digest_into() {
  local __o="$1" __salt="$2" __val="$3" __d=''
  eval "$__o="
  [ -n "$CLAUDE_AUTH_DIGEST_CMD" ] || return 1
  # The pipe is safe here for the reason the matcher block gives: the SIGPIPE hazard is an
  # early-exiting consumer, and a digest tool reads to EOF by construction.
  __d=$(printf '%s' "$__salt$__val" | claude_auth_bounded "$CLAUDE_AUTH_DIGEST_BOUND" $CLAUDE_AUTH_DIGEST_CMD 2>/dev/null)
  __d=${__d%% *}
  case "$__d" in
    *[!0-9a-f]*|'') return 1 ;;
  esac
  [ "${#__d}" -eq 64 ] || return 1
  eval "$__o=\$__d"
}

# claude_auth_probe_salt_into <outvar>: a per-run salt. It is not a secret and needs no
# cryptographic source — its only job is to keep the digest from being a stable
# fingerprint of the credential across runs.
claude_auth_probe_salt_into() {
  eval "$1=\"cqlite-3733-\$\$-\${RANDOM}\${RANDOM}-\${SECONDS}\""
}

# ---- shell tracing: OFF ACROSS EVERY SECRET-BEARING PATH ---------------------------
# `bash -x` (or an inherited `set -x`) prints every expanded assignment and every
# command's ARGV *before* the command runs — which is before the redaction boundary can
# see anything at all. Measured on the code this replaced: `bash -x claude-auth-capability.sh
# --auth` printed the token 8 times, `--report` 16, `--fix-tmux-env` 15 (the
# `[ -n "$CLAUDE_AUTH_SECRET" ]` test, the `env KEY=<token> … claude -p` argv, the
# `tmux setenv -g KEY <token>` argv).
#
# THIS IS AN ACCIDENT ROUTE, NOT A HOSTILE-INVOKER ONE, and that is what makes it a defect
# rather than an out-of-model note: an operator debugging a failing preflight reaches for
# `bash -x` precisely BECAUSE this check is what is failing, and CI harnesses set `set -x`
# wholesale. The rule for this file is that the token is never printed ANYWHERE.
#
# DEPTH-COUNTED, because the wrapped entry points call each other (a verdict function calls
# the file reader), and a save/restore pair that forgot that would restore on the INNER
# return and trace the rest of the outer call. RESTORED, never merely turned off: this file
# is SOURCED by bootstrap, so seizing a caller's `set -x` is the same class of rudeness as
# replacing its trap.
CLAUDE_AUTH_XTRACE_DEPTH=0
CLAUDE_AUTH_XTRACE_WAS=0
claude_auth_xtrace_off() {
  if [ "$CLAUDE_AUTH_XTRACE_DEPTH" -eq 0 ]; then
    case "$-" in *x*) CLAUDE_AUTH_XTRACE_WAS=1 ;; *) CLAUDE_AUTH_XTRACE_WAS=0 ;; esac
  fi
  CLAUDE_AUTH_XTRACE_DEPTH=$((CLAUDE_AUTH_XTRACE_DEPTH + 1))
  set +x
  return 0
}
claude_auth_xtrace_restore() {
  if [ "$CLAUDE_AUTH_XTRACE_DEPTH" -gt 0 ]; then
    CLAUDE_AUTH_XTRACE_DEPTH=$((CLAUDE_AUTH_XTRACE_DEPTH - 1))
  fi
  if [ "$CLAUDE_AUTH_XTRACE_DEPTH" -eq 0 ] && [ "$CLAUDE_AUTH_XTRACE_WAS" = 1 ]; then
    set -x
  fi
  return 0
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
claude_auth_redact__untraced() {
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
claude_auth_read_key_into__untraced() {
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
claude_auth_verdict_into__untraced() {
  local __ov="$1" __od="$2" __op="${3:-}"
  local __file='' __state='' __tok='' __cfg='' __out='' __rc=0 __alt=''
  eval "$__ov=UNMEASURED"; eval "$__od="
  [ -z "$__op" ] || eval "$__op="
  # Read BEFORE the probe runs, from THIS process's environment — which is the environment
  # the probe inherits everything but the two scrubbed keys from.
  claude_auth_alt_credentials_into __alt

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
    eval "$__od='no timeout/gtimeout on PATH can enforce a HARD bound (neither --kill-after= nor -k is accepted) — a SIGTERM-only bound does not bound a child that ignores SIGTERM, so this refuses to run the network probe rather than hang the provisioning entry point'"
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

  # ---- LIMITATION 2 of 5 (#3733): THIS SCRUBS ONE CREDENTIAL AND LEAVES THE OTHERS. Only
  # $CLAUDE_AUTH_TOKEN_KEY is neutralised and re-supplied from the persisted value;
  # ANTHROPIC_API_KEY, ANTHROPIC_AUTH_TOKEN, CLAUDE_CODE_USE_BEDROCK and
  # CLAUDE_CODE_USE_VERTEX are inherited from this process untouched, and `claude`
  # authenticates from any of them. So a returned sentinel means SOME credential in this
  # environment worked — not that the persisted one did, which is the claim the old
  # `VERIFIED` state made and could not support. NOT scrubbed, on the standing ruling that
  # this file reports rather than certifies: silently changing what the probe authenticates
  # with would be a behaviour change hiding behind a report, and the honest move is to NAME
  # what was present (see `claude_auth_alt_credentials_into`, whose output goes into the
  # PROBE-ANSWERED detail).
  #
  # NOT A PIPE. `__out=$(...)` then `__rc=$?` on its own line. ONE invocation, because
  # there is no longer a bound-without-escalation form to branch on: the resolver refuses
  # rather than hand back a SIGTERM-only `timeout`.
  __out=$(env -u "$CLAUDE_AUTH_TOKEN_KEY" -u BASH_ENV -u ENV -u SHELLOPTS -u BASHOPTS \
    "$CLAUDE_AUTH_TOKEN_KEY=$__tok" "$CLAUDE_AUTH_CONFIG_KEY=$__cfg" \
    "$CLAUDE_AUTH_TIMEOUT_BIN" "${CLAUDE_AUTH_TIMEOUT_KILL_ARGS[@]}" "$CLAUDE_AUTH_PROBE_BOUND" \
    claude -p "$CLAUDE_AUTH_PROMPT" 2>&1)
  __rc=$?
  claude_auth_probe_cleanup
  claude_auth_probe_restore_traps
  [ -z "$__op" ] || eval "$__op=\"\$(claude_auth_redact \"\$__out\")\""

  if [ "$__rc" -eq 0 ] && claude_auth_contains "$__out" "$CLAUDE_AUTH_SENTINEL"; then
    eval "$__ov=PROBE-ANSWERED"
    # THE WORDING IS THE WHOLE DEMOTION. This used to say the persisted credential
    # "authenticated", which is a claim about WHICH credential worked — and nothing here
    # observes that (LIMITATION 2 at the `env` call above). What was observed is that a run
    # whose environment CARRIED the persisted value answered. The alternate credentials
    # present in that environment are NAMED, so the reader can see when another way in was
    # available; with none present the line says so, because "none was found" and "nobody
    # looked" are different facts and only one of them is this file's own reporting gap.
    # LIMITATION REFERENCE FIRST: `claude_auth_redact` truncates the detail at 500
    # characters, so a reference at the end of a long sentence is silently cut — a
    # documented limitation a reader cannot find is not documented.
    if [ -n "$__alt" ]; then
      eval "$__od=\"LIMITATION 2 of 5 — THIS DOES NOT SAY THE PERSISTED VALUE IS WHAT WORKED: these alternate credentials were in the probe's environment too and are NOT scrubbed: \$__alt. What was observed: a cold, non-interactive 'claude -p' run whose environment CARRIED the \$CLAUDE_AUTH_TOKEN_KEY persisted in \$__file answered (rc 0 AND the sentinel returned) against a FRESH empty config dir\""
    else
      eval "$__od=\"LIMITATION 2 of 5 — no alternate credential (\$CLAUDE_AUTH_ALT_CRED_KEYS) was set here, which is an OBSERVATION about this process and not a proof that the persisted value is what worked. What was observed: a cold, non-interactive 'claude -p' run whose environment CARRIED the \$CLAUDE_AUTH_TOKEN_KEY persisted in \$__file answered (rc 0 AND the sentinel returned) against a FRESH empty config dir\""
    fi
    return 0
  fi
  # ---- the probe did not succeed. WHY it did not decides the verdict. ----------------
  # See the matcher block at the top of this file: only a POSITIVELY IDENTIFIED rejection
  # earns FAILED. `__ov` is already UNMEASURED from the top of the function, so every
  # branch below except the rejection one simply names its cause.
  if [ "$__rc" = 124 ] || [ "$__rc" = 137 ]; then
    eval "$__od=\"the probe exceeded its \${CLAUDE_AUTH_PROBE_BOUND}s bound and was killed — the credential is UNKNOWN, not ok\""
    return 0
  fi
  # TRANSPORT FIRST (step 2), so a message naming both a transport failure and a rejection
  # takes the safe answer: an unreachable API says nothing about the credential.
  if claude_auth_matches_ci "$__out" "$CLAUDE_AUTH_NETWORK_RE"; then
    eval "$__od=\"the probe could not reach the API (\$(claude_auth_redact \"\$__out\")) — the credential is UNKNOWN, not ok\""
    return 0
  fi
  # SERVICE BEFORE REJECTION, and the order is the safe-tie rule itself (see step 3/4 of
  # the decision order at the top of this file): a response naming BOTH a rate limit and an
  # authentication error says nothing certain about the credential, and FAILED's remedy is
  # "replace the VALUE".
  if claude_auth_matches_ci "$__out" "$CLAUDE_AUTH_SERVICE_RE"; then
    eval "$__od=\"the API refused for a reason that is NOT credential rejection — a rate limit, an outage, an overload or an exhausted quota (rc=\$__rc): \$(claude_auth_redact \"\$__out\") — the credential is UNKNOWN, not rejected; retry rather than replace it\""
    return 0
  fi
  if claude_auth_matches_ci "$__out" "$CLAUDE_AUTH_REJECT_RE"; then
    eval "$__ov=FAILED"
    eval "$__od=\"the \$CLAUDE_AUTH_TOKEN_KEY persisted in \$__file was REJECTED (rc=\$__rc): \$(claude_auth_redact \"\$__out\")\""
    return 0
  fi
  if [ "$__rc" -eq 0 ]; then
    eval "$__od=\"'claude -p' exited 0 but did NOT return the sentinel, so it did not answer — which is not evidence that the credential was rejected: \$(claude_auth_redact \"\$__out\")\""
    return 0
  fi
  eval "$__od=\"'claude -p' exited \$__rc and its output identifies NO authentication rejection, so what happened to the credential is UNKNOWN: \$(claude_auth_redact \"\$__out\") — read the output before replacing anything\""
}

# ---- (b) claude-tmux-env: does the credential REACH a tmux-spawned pane? -----------
# claude_tmux_env_verdict_into <outvar_verdict> <outvar_detail>
#
# THE DIMENSION THAT ACTUALLY FAILED IN THE FIELD. A pane's environment is the tmux
# SERVER's, fixed at server start, so a server predating provisioning hands out panes with
# neither variable however correct the disk is. A STALE server value is a distinct and
# worse state than none — the NOT-SYSTEM-WIDE analogue from #3414 — because everything
# looks provisioned and the credential is simply the wrong one.
claude_tmux_env_verdict_into__untraced() {
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
  # missing here, so a macOS host could emit VERIFIED, which was then an [ok] and so was
  # what `--strict` read (#3414: scoping a platform out is not the same as passing it).
  # Neither line reaches `ok()` since #3733, so the guard now keeps the TEXT honest rather
  # than keeping a false [ok] out of a strict run.
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
  # A LIVE SERVER CAN WEDGE, so the read needs the same HARD bound the network probe needs,
  # and for the same reason: where the call cannot be bounded it is not made at all. Checked
  # HERE rather than read from the bounded runner's rc, because the read below runs in a
  # command substitution — a subshell — where any global the runner set is discarded.
  if ! claude_auth_resolve_timeout; then
    eval "$__od='no timeout/gtimeout on PATH can enforce a HARD bound (neither --kill-after= nor -k is accepted) — refusing to run an UNBOUNDED tmux read, because a server that accepts a connection and never answers would hang this provisioning entry point indefinitely'"
    return 0
  fi
  # WHOSE SERVER (see the identity block above). Under the documented `sudo` invocation the
  # server that matters belongs to the INVOKING agent, not to root; where that identity
  # cannot be resolved nothing is measured, because answering about the current UID's server
  # is how this check certified a box that could not start a lane.
  claude_auth_resolve_tmux_identity
  if [ "$CLAUDE_AUTH_TMUX_IDENTITY" = ambiguous ]; then
    eval "$__od=\"the tmux server to inspect could not be identified: \$CLAUDE_AUTH_TMUX_IDENTITY_WHY — this runs under sudo, and a pane is spawned by the INVOKING agent's server, so answering about this process's own UID would certify the wrong one\""
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
  # ---- LIMITATION 5 of 5 (#3733): THIS READS THE SERVER'S GLOBAL ENVIRONMENT, AND
  # NOTHING ELSE. `show-environment -g` prints the server's global table; it does not spawn
  # a pane, so what a pane would ACTUALLY receive is not observed here. A session or window
  # environment entry (`show-environment` without `-g`, `new-session -e`), an
  # `update-environment` list, or a per-pane override can all differ from the global table,
  # and every one of them is invisible to this call. Documented rather than fixed: spawning
  # a pane on the operator's LIVE server to find out would mean creating a session on a box
  # whose sessions are lanes, which is a worse thing for a report to do than being
  # incomplete. The cold path DOES spawn a pane — on its own throwaway server — and carries
  # LIMITATION 1 instead.
  local __errf=''
  if __errf=$(mktemp "${TMPDIR:-/tmp}/cqlite-tmuxenv.XXXXXX" 2>/dev/null) && [ -f "$__errf" ]; then
    # REGISTERED WITH THE PROBE LIFECYCLE, not merely deleted on the success path. Bounding
    # this read (above) turned the window between `mktemp` and `rm` from microseconds into
    # up to CLAUDE_AUTH_TMUX_OP_BOUND seconds against a wedged server, and an interrupt in
    # that window used to leave a `cqlite-tmuxenv.*` file behind in a directory we do not
    # own — measured, from this suite's own interrupted run. Same machinery, same reason, as
    # the two probes' working directories; cleanup is keyed on what is REGISTERED, so the
    # socket and directory halves are simply skipped here.
    CLAUDE_AUTH_PROBE_FILE="$__errf"
    claude_auth_probe_arm_traps
    __out=$(claude_auth_tmux_run "$CLAUDE_AUTH_TMUX_OP_BOUND" show-environment -g 2>"$__errf")
    __rc=$?
    __err=$(cat "$__errf" 2>/dev/null)
    claude_auth_probe_cleanup
    claude_auth_probe_restore_traps
  else
    # No temp file: keep the ONE invocation and say the cause was not captured, rather than
    # taking a second reading of a different moment.
    __out=$(claude_auth_tmux_run "$CLAUDE_AUTH_TMUX_OP_BOUND" show-environment -g 2>/dev/null)
    __rc=$?
    __err='(stderr not captured: no temporary file could be created)'
  fi
  # THE BOUND FIRING IS ITS OWN OUTCOME, and it must be read BEFORE the error text is
  # classified: a server that accepts the connection and never answers produces no stderr at
  # all, so the wordings below would fall through to "failed for a reason that is not a
  # missing server" and name no cause. What was learned is nothing, and the verdict says so.
  if claude_auth_bound_fired "$__rc"; then
    eval "$__od=\"the running tmux server did not answer 'show-environment -g' within \${CLAUDE_AUTH_TMUX_OP_BOUND}s and the read was killed (a server that accepts a connection but never responds) — what its panes receive is UNKNOWN\""
    return 0
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
  # directory, and THAT is the first-run picker this issue exists for. So SERVER-CARRIES-BOTH
  # requires an AFFIRMATIVE match — equal to the persisted value AND the directory exists.
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
  #
  # ---- LIMITATION 3 of 5 (#3733): THIS TEST IS EVALUATED AS *THIS* PROCESS. Under the
  # documented `sudo bash scripts/bootstrap-agent-machine.sh --yes` invocation that is ROOT,
  # while the account that will actually spawn the lane is the invoking agent — so a
  # directory root can stat is reported as existing even when the agent cannot read or write
  # it, which is the un-onboarded first-run picker all over again. NOT delegated, and the
  # reason is that a delegated `test -d` is a THIRD identity-dependent probe to bound,
  # authorize and interpret, and the ruling for this file is to report rather than to grow.
  # The `*-CARRIES-BOTH`/`*-DELIVERS-BOTH` details say "as seen by THIS process" for exactly
  # this reason. (Note the tmux READ is delegated — see the identity block — so this is a
  # gap in one predicate, not in the section's posture.)
  if [ ! -d "$__scfg" ]; then
    eval "$__ov=SERVER-CONFIG-NODIR"
    eval "$__od=\"the server's \$CLAUDE_AUTH_CONFIG_KEY MATCHes \$__file but that directory does not exist (or cannot be read as one): \$__scfg — a pane gets a config dir claude will treat as un-onboarded\""
    return 0
  fi
  eval "$__ov=SERVER-CARRIES-BOTH"
  # Limitation references FIRST — the detail is truncated at 500 characters (see the note
  # in claude_tmux_cold_verdict_into).
  eval "$__od=\"LIMITATION 5 of 5 (the server's GLOBAL table only — no pane was spawned) + LIMITATION 3 of 5 (the dir exists TO THIS PROCESS). The running server's global environment carries a \$CLAUDE_AUTH_TOKEN_KEY MATCHing \$__file and a \$CLAUDE_AUTH_CONFIG_KEY MATCHing it whose directory exists\""
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
CLAUDE_AUTH_PROBE_FILE=''
CLAUDE_AUTH_PROBE_PREV_TRAPS=''

# THE PROBE LIFECYCLE HELPERS NOW HAVE THREE REGISTRANTS, NOT TWO, AND ONE OF THEM IS NOT A
# PROBE — the `--auth` probe (a throwaway CLAUDE_CONFIG_DIR), the cold-start tmux probe (a
# working directory AND a server socket), and the LIVE READ's stderr temp file, which joined
# when bounding that read widened its own leak window from microseconds to seconds. They were
# named `..._cold_probe_...` when only the cold one existed; a name that says "cold" while
# two other paths depend on it is a comment that lies in the symbol table, and "BOTH probes"
# was the same defect one revision later. Cleanup is keyed on what is REGISTERED, so each
# half is simply skipped when nothing was armed for it.
claude_auth_probe_cleanup() {
  if [ -n "$CLAUDE_AUTH_PROBE_SOCKET" ]; then
    # rc is deliberately ignored: tmux `exit-empty` means the server may already be gone,
    # which is a SUCCESSFUL cleanup, not a failure.
    # BOUNDED LIKE EVERY OTHER TMUX CALL, and this one runs from an EXIT/signal trap, where
    # an unbounded call against a wedged server would hang the caller at exit — the worst
    # place for it. When no bound can be established the call is SKIPPED rather than run
    # unbounded (a hung exit trap is worse than a throwaway server that `exit-empty` will
    # end); that branch is unreachable by construction, because a socket is registered only
    # after `claude_auth_resolve_timeout` has already succeeded.
    claude_auth_bounded "$CLAUDE_AUTH_TMUX_OP_BOUND" \
      ${CLAUDE_AUTH_TMUX_PREFIX[@]+"${CLAUDE_AUTH_TMUX_PREFIX[@]}"} \
      tmux -S "$CLAUDE_AUTH_PROBE_SOCKET" kill-server >/dev/null 2>&1
    CLAUDE_AUTH_PROBE_SOCKET=''
  fi
  if [ -n "$CLAUDE_AUTH_PROBE_DIR" ]; then
    rm -rf "$CLAUDE_AUTH_PROBE_DIR"
    CLAUDE_AUTH_PROBE_DIR=''
  fi
  if [ -n "$CLAUDE_AUTH_PROBE_FILE" ]; then
    rm -f "$CLAUDE_AUTH_PROBE_FILE"
    CLAUDE_AUTH_PROBE_FILE=''
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

# claude_tmux_cold_probe_into <ov_ok> <ov_tok> <ov_toklen> <ov_match> <ov_cfg> <ov_why>
#                             <tok> <cfg>
# ov_ok is 1 only when a pane actually reported. An EMPTY <tok>/<cfg> means "a cold session
# would receive nothing here", so the variable is left UNSET for the probe — which is what
# pam_env does with an absent assignment.
# ov_match is the IDENTITY of the delivered token, three-valued: `match` (its salted digest
# equals the persisted value's), `differs`, or `unmeasured` (no digest could be computed on
# one side or the other). `unmeasured` is never read as `match` — see the caller.
claude_tmux_cold_probe_into() {
  local __ok="$1" __otok="$2" __olen="$3" __omatch="$4" __ocfg="$5" __owhy="$6"
  local __ptok="$7" __pcfg="$8"
  local __dir='' __res='' __sock='' __rc=0 __salt='' __expdig=''
  eval "$__ok=0"; eval "$__otok=unset"; eval "$__olen=0"; eval "$__omatch=unmeasured"
  eval "$__ocfg="; eval "$__owhy="

  if ! claude_auth_resolve_timeout; then
    eval "$__owhy='no timeout/gtimeout on PATH can enforce a HARD bound (neither --kill-after= nor -k is accepted) — refusing to start a tmux probe whose bound could not kill a wedged child'"
    return 0
  fi
  # THE IDENTITY COMPARISON IS A PRECONDITION OF THE PROBE, not a step inside it: without a
  # digest tool the only available comparison is by LENGTH, which a same-length
  # substitution satisfies. Refusing is the non-permissive answer and the cause is named.
  if ! claude_auth_resolve_digest; then
    eval "$__owhy='no sha256 digest tool (sha256sum / shasum -a 256) on PATH, so what a pane RECEIVES could not be compared to the persisted value by VALUE — refusing rather than falling back to a LENGTH comparison, which a same-length substitution satisfies'"
    return 0
  fi
  claude_auth_probe_salt_into __salt
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
  # ---- ROOT CREATES EVERYTHING IT OWNS **BEFORE** THE HANDOVER BELOW ----------------
  # THE ORDER OF THESE TWO STEPS IS A SECURITY PROPERTY, NOT HOUSEKEEPING (#3733, the
  # limitation formerly numbered 4 of 5). This `cat` used to sit AFTER the `chown -R`, so
  # root wrote into a directory it had ALREADY given away — and on this fleet EVERY LANE
  # RUNS AS ONE USER, so the recipient is a PEER LANE, which can plant a symlink at
  # `probe.sh` and have ROOT truncate and overwrite an arbitrary file. That is a
  # NON-INVOKER route and therefore a defect, not a documented limitation of a report: it
  # is not a claim that could merely be wrong, and it exists whatever the verdict line says.
  # Written first, the file is created while the directory is still this uid's own 0700
  # `mktemp -d`, so there is no window in which another party can interpose a path.
  # SECOND EFFECT, VERIFIED NOT ASSUMED: because `chown -R` now runs AFTER the write, it
  # covers `probe.sh` too, so the file's owner becomes the delegate and a restrictive umask
  # no longer strands it root-readable-only. Previously it kept this uid's ownership at
  # root's umask, and at 0077 a delegated `sh probe.sh` could not read it — reported as "the
  # isolated pane did not report", a true statement with a misleading cause. Both halves are
  # closed by the one reorder.
  #
  # The pane script reports DELIVERY, never the value: set/unset, a LENGTH, a SALTED DIGEST
  # and the config directory (a path, not a secret). Nothing it writes carries the
  # credential — the digest is salted per run, so it identifies the delivered value only by
  # comparison against a digest of the persisted one taken with the SAME salt.
  cat >"$__dir/probe.sh" <<'CLAUDE_AUTH_PROBE'
#!/bin/sh
t="${CLAUDE_CODE_OAUTH_TOKEN-}"
# THE PANE REPORTS DELIVERY, NEVER THE VALUE: set/unset, a length, a SALTED DIGEST, and the
# config directory (a path, not a secret). $CQLITE_AUTH_PROBE_DIGEST is unquoted because it
# is a command WORD LIST (`shasum -a 256`) resolved by the parent from a closed literal set.
d=''
if [ -n "$t" ] && [ -n "${CQLITE_AUTH_PROBE_DIGEST-}" ]; then
  d=$(printf '%s' "${CQLITE_AUTH_PROBE_SALT-}$t" | $CQLITE_AUTH_PROBE_DIGEST 2>/dev/null)
  d=${d%% *}
fi
{
  printf 'tok=%s\n' "${CLAUDE_CODE_OAUTH_TOKEN+set}"
  printf 'toklen=%s\n' "${#t}"
  printf 'tokdig=%s\n' "$d"
  printf 'cfg=%s\n' "${CLAUDE_CONFIG_DIR-}"
  printf 'end\n'
} >"$1"
CLAUDE_AUTH_PROBE
  # THE WRITE IS REQUIRED TO HAVE WORKED, and the check is on the RESULT rather than on an
  # exit status: "cat exited 0" and "the script is there" are different claims (the same
  # rule the isolated config's `chmod` verification follows). Unchecked, a failed write
  # surfaced later as "the isolated pane did not report" — a cause that sends the reader
  # looking at tmux.
  if [ ! -s "$__dir/probe.sh" ]; then
    rm -rf "$__dir"
    eval "$__owhy='the isolated probe pane script could not be written into the private working directory (no space, or the write failed) — refusing rather than starting a probe whose pane has nothing to run'"
    return 0
  fi

  # A DELEGATED PROBE MUST BE ABLE TO WRITE ITS OWN WORKING DIRECTORY. `mktemp -d` gives us
  # a 0700 directory owned by THIS uid, and a tmux started as the invoking agent could
  # neither create the socket in it nor write the pane report. The probe is delegated for the
  # same reason the live read is: a per-user tmux config is exactly what can substitute the
  # credential, so a probe run as root measures ROOT's would-be server and says nothing about
  # the agent's. A FAILED handover is a REFUSAL, never a quiet fall back to a root-run probe
  # whose answer would be about the wrong user.
  #
  # ---- LIMITATION 4 of 5 (#3733) — FIXED, and the slot is KEPT AS A RECORD rather than
  # renumbered. It read: this handover PRECEDES the heredoc that writes `probe.sh`, so that
  # one file is not covered by it and keeps this uid's ownership at this uid's umask. It was
  # first filed as a umask problem and documented alongside the four PROXY limitations, and
  # that classification was WRONG: the ruling that excused those ("a report cannot break a
  # working box") does not reach this one, because root writing into a directory it has
  # already given away is not a claim at all — it is a same-uid peer's opportunity to
  # interpose a symlink and have ROOT overwrite an arbitrary file. A hazard that exists
  # whatever the output says is a defect. FIXED by writing `probe.sh` above, before this
  # handover; the umask half closed with it, because `-R` now covers the file. The number is
  # not reused and 1/2/3/5 keep theirs, so every reference written while it was live still
  # resolves. Pinned behaviourally AND structurally by section 37 of
  # scripts/tests/test_claude_auth_capability.sh.
  if [ "$CLAUDE_AUTH_TMUX_IDENTITY" = delegate ]; then
    if ! chown -R "$CLAUDE_AUTH_TMUX_IDENTITY_USER" "$__dir" 2>/dev/null; then
      rm -rf "$__dir"
      eval "$__owhy=\"the probe's private working directory could not be handed to the invoking agent (\$CLAUDE_AUTH_TMUX_IDENTITY_USER), so a tmux started as that login could not write into it — refusing rather than measuring the wrong user's cold start\""
      return 0
    fi
  fi
  CLAUDE_AUTH_PROBE_DIR="$__dir"; CLAUDE_AUTH_PROBE_SOCKET="$__sock"
  claude_auth_probe_arm_traps

  # ---- LIMITATION 1 of 5 (#3733): WHAT FOLLOWS OBSERVES TMUX PROPAGATION, NOT pam_env
  # DELIVERY. The two credential variables are scrubbed and RE-SUPPLIED HERE from the values
  # this process READ out of the pam_env file — so the throwaway server is handed them by us,
  # and what is then measured is whether a tmux server passes its own start environment to a
  # pane. It is NOT whether pam_env would deliver those values to a login session: a
  # `/etc/environment` line pam_env silently drops (a grammar this parser reads more
  # permissively than pam does, a pam_env misconfiguration, a `readenv=0`) is invisible here,
  # and the cold state would still report a delivery. Deliberate, and it is the only shape
  # available: measuring pam_env would mean CREATING a PAM session, which needs privilege
  # this may not have and would be a login on the operator's box. The propagation half is
  # still the half that failed in the field (fact 4), which is why the probe exists at all —
  # what changed on #3733 is that the state it produces no longer claims the other half.
  local -a __e=(env -u BASH_ENV -u ENV -u SHELLOPTS -u BASHOPTS -u TMUX -u TMUX_PANE
                -u "$CLAUDE_AUTH_TOKEN_KEY" -u "$CLAUDE_AUTH_CONFIG_KEY"
                "CQLITE_AUTH_PROBE_SALT=$__salt" "CQLITE_AUTH_PROBE_DIGEST=$CLAUDE_AUTH_DIGEST_CMD")
  [ -z "$__ptok" ] || __e+=("$CLAUDE_AUTH_TOKEN_KEY=$__ptok")
  [ -z "$__pcfg" ] || __e+=("$CLAUDE_AUTH_CONFIG_KEY=$__pcfg")
  # NOT A PIPE: the command runs on its own line and $? is read on the next. The scrub
  # `env` sits INSIDE the bound (it is the thing being run), and any delegation prefix sits
  # in front of it, so the whole chain is what the bound kills.
  claude_auth_bounded "$CLAUDE_AUTH_TMUX_PROBE_BOUND" \
    ${CLAUDE_AUTH_TMUX_PREFIX[@]+"${CLAUDE_AUTH_TMUX_PREFIX[@]}"} "${__e[@]}" \
    tmux -S "$__sock" new-session -d -s cqlite-authprobe "sh '$__dir/probe.sh' '$__res'" >/dev/null 2>&1
  __rc=$?
  if [ "$__rc" -ne 0 ]; then
    claude_auth_probe_cleanup; claude_auth_probe_restore_traps
    eval "$__owhy=\"an isolated throwaway tmux server could not be started on a private socket (rc=\$__rc)\""
    return 0
  fi

  # `new-session -d` returns as soon as the session exists, so the pane may not have
  # written yet. The wait is bounded by the SAME timeout binary rather than by a counted
  # `sleep` loop, so a host whose sleep has no sub-second form cannot stretch it.
  claude_auth_bounded "$CLAUDE_AUTH_TMUX_PROBE_BOUND" \
    sh -c 'while :; do grep -q "^end$" "$1" 2>/dev/null && exit 0; sleep 0.2; done' _ "$__res" >/dev/null 2>&1
  __rc=$?
  if [ "$__rc" -ne 0 ]; then
    claude_auth_probe_cleanup; claude_auth_probe_restore_traps
    eval "$__owhy=\"the isolated pane did not report within \${CLAUDE_AUTH_TMUX_PROBE_BOUND}s, so what a new server would deliver is UNKNOWN\""
    return 0
  fi

  local __rtok='' __rlen='' __rcfg='' __rdig='' __match=unmeasured
  __rtok=$(sed -n 's/^tok=//p' "$__res" 2>/dev/null | tail -1)
  __rlen=$(sed -n 's/^toklen=//p' "$__res" 2>/dev/null | tail -1)
  __rdig=$(sed -n 's/^tokdig=//p' "$__res" 2>/dev/null | tail -1)
  __rcfg=$(sed -n 's/^cfg=//p' "$__res" 2>/dev/null | tail -1)
  claude_auth_probe_cleanup; claude_auth_probe_restore_traps
  case "$__rlen" in ''|*[!0-9]*) __rlen=0 ;; esac
  # THE IDENTITY, decided here rather than by the caller, so no caller can read a
  # could-not-tell as a match. Both digests are salted with the SAME per-run salt; an empty
  # or unhashable side stays `unmeasured`, which is UNMEASURED-class upstream.
  if [ -n "$__ptok" ] && [ -n "$__rdig" ] && claude_auth_digest_into __expdig "$__salt" "$__ptok"; then
    if [ "$__rdig" = "$__expdig" ]; then __match=match; else __match=differs; fi
  fi
  eval "$__omatch=\$__match"
  eval "$__otok=\${__rtok:-unset}"; eval "$__olen=\$__rlen"; eval "$__ocfg=\$__rcfg"
  eval "$__ok=1"
}

# claude_tmux_cold_verdict_into <outvar_verdict> <outvar_detail> <env-file>
# States: COLD-START-DELIVERS-BOTH | COLD-START-MISSING | COLD-START-INCOMPLETE |
#         COLD-START-NODIR | NO-SERVER (UNMEASURED-class: the isolated probe could not run).
# NONE of them certifies anything (#3733) — an enumeration in a comment is exactly the prose
# that rots after a rename, and this one said `VERIFIED` for a run after the token was gone.
# The COLD-START-* names are deliberately distinct from the SERVER-* ones: "a live server
# that lacks the credential" and "a would-be server that would lack it" are different
# operator actions, and a pasted log has to keep them apart.
claude_tmux_cold_verdict_into() {
  local __ov="$1" __od="$2" __file="$3"
  local __tok='' __state='' __cfg='' __cfgstate=''
  # The out-var names here must NOT collide with the callee's own local parameter names:
  # `eval "$__ok=1"` on a shadowed `__ok` evaluates `0=1`. Hence the `__pr*` prefix.
  local __prok=0 __prtok='' __prlen=0 __prmatch='' __prcfg='' __prwhy=''
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

  claude_tmux_cold_probe_into __prok __prtok __prlen __prmatch __prcfg __prwhy "$__tok" "$__cfg"
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
  # THE DELIVERED VALUE MUST BE THE PERSISTED VALUE, COMPARED AS A VALUE. This was a LENGTH
  # comparison, and length equality is not value equality: one `set-environment` line in a
  # tmux config substituting a different token of the same length yielded VERIFIED — then the
  # verdict that certified a fresh box, a state that no longer exists (#3733). The
  # observation below is still worth making. The comparison is by salted digest and neither value
  # is printed (see the digest block near the top). `unmeasured` — no digest computable on
  # one side — is NOT read as a match: an unmeasured identity is UNMEASURED-class, and this
  # branch says which of the two it was.
  if [ "$__prmatch" != match ]; then
    if [ "$__prmatch" = differs ]; then
      eval "$__ov=NO-SERVER"
      eval "$__od=\"the isolated pane received a \$CLAUDE_AUTH_TOKEN_KEY that does not match the persisted value (compared by salted digest; neither value printed) — something between \$__file and the pane SUBSTITUTES the credential, so this measured something other than the persisted source — UNMEASURED-class\""
    else
      eval "$__ov=NO-SERVER"
      eval "$__od=\"the isolated pane's \$CLAUDE_AUTH_TOKEN_KEY could not be compared to the persisted value at all (no digest was computable on one side), so whether a new server would deliver the RIGHT credential is UNKNOWN — UNMEASURED-class\""
    fi
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
  # exist", which is deliberately the NON-permissive answer. And the same LIMITATION 3 of 5
  # (#3733) applies here as at the live-path site — `[ -d ]` runs as THIS process, which
  # under the documented sudo invocation is root, so this says the directory exists TO US and
  # not that the agent that will spawn the lane can use it. Two sites, one limitation,
  # marked at both: a reader lands on whichever one they are reading.
  if [ ! -d "$__prcfg" ]; then
    eval "$__ov=COLD-START-NODIR"
    eval "$__od=\"a throwaway server started from \$__file delivers both variables, but the \$CLAUDE_AUTH_CONFIG_KEY it delivers does not exist as a directory: \$__prcfg — claude will treat it as un-onboarded\""
    return 0
  fi
  eval "$__ov=COLD-START-DELIVERS-BOTH"
  # THE DETAIL IS BUDGETED, not merely written: `claude_auth_redact` truncates at 500
  # characters, so a limitation reference buried in a long sentence is silently cut — which
  # is a documented limitation that a reader cannot find. The references come FIRST.
  eval "$__od=\"LIMITATION 1 of 5 (tmux propagation, NOT pam_env delivery: the probe supplies the values itself) + LIMITATION 3 of 5 (the dir exists TO THIS PROCESS). No live server, so measured COLD: an isolated throwaway server on a private socket, handed what \$__file holds with the inherited credential scrubbed, delivered BOTH \$CLAUDE_AUTH_TOKEN_KEY and an existing \$CLAUDE_AUTH_CONFIG_KEY to a pane\""
}

# claude_tmux_show_key_into <outvar_value> <outvar_state> <show-environment-output> <key>:
# `tmux show-environment -g` prints `KEY=value` for a set variable and `-KEY` for one
# explicitly removed. Both "not listed" and "listed as removed" are `absent` — a pane
# receives nothing either way.
claude_tmux_show_key_into() {
  local __ov="$1" __os="$2" __text="$3" __k="$4"
  local __line='' __hit='' __found=0 __removed=0
  eval "$__ov="; eval "$__os=absent"
  # A LINE WALK, not `printf | grep`: `grep -x`/`grep -m1` EXIT ON THE FIRST MATCH and the
  # producer then takes SIGPIPE, so under `pipefail` a PRESENT key read as ABSENT once the
  # server environment passed one pipe buffer (see the matcher block above).
  #
  # A HERE-STRING, not a PIPE and not repeated string slicing. The pipe is what the defect
  # was. Slicing the remainder (`${rest#*$'\n'}`) is safe but QUADRATIC — measured at 41
  # SECONDS for a 5000-variable environment, i.e. it would have replaced a wrong answer
  # with an unusable one on exactly the heavily populated server this fix is for. A
  # here-string redirect keeps the loop in THIS shell (a piped `while read` runs in a
  # subshell and its writes are discarded) and reads to EOF, so there is no early exit to
  # lose a race with. Same case, after: 40 ms.
  #
  # `__found` is tracked SEPARATELY from `__hit` because `KEY=` is a legitimate EMPTY
  # assignment, and collapsing it onto "not found" would be the same two-valued read one
  # level down; the callers reject present-but-empty themselves, with their own wording.
  while IFS= read -r __line; do
    if [ "$__line" = "-$__k" ]; then __removed=1; fi
    case "$__line" in
      "$__k"=*) if [ "$__found" = 0 ]; then __hit=${__line#"$__k"=}; __found=1; fi ;;
    esac
  done <<<"$__text"
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
claude_auth_fix_tmux_env__untraced() {
  local __file='' __tok='' __state='' __cfg='' __cfgstate='' __cfgsrc='' __cfgrc=0 rc=0
  if ! claude_auth_env_file_into __file; then
    printf 'claude-auth: fix REFUSED (the TEST-ONLY seam is set without CQLITE_BOOTSTRAP_TEST_MODE=1)\n'
    return 1
  fi
  if ! command -v tmux >/dev/null 2>&1; then
    printf 'claude-auth: fix SKIPPED (no `tmux` on PATH — nothing to seed)\n'
    return 1
  fi
  # Same rule as the read: a `setenv -g` against a wedged server hangs forever, so where the
  # call cannot be bounded it is not made.
  if ! claude_auth_resolve_timeout; then
    printf 'claude-auth: fix REFUSED (no timeout/gtimeout on PATH can enforce a HARD bound, and an UNBOUNDED `tmux setenv -g` against a server that never answers would hang this entry point)\n'
    return 1
  fi
  # Same identity rule as the read, and it matters MORE here: seeding root's server while
  # the agent's stays broken reports success and changes nothing that matters.
  claude_auth_resolve_tmux_identity
  if [ "$CLAUDE_AUTH_TMUX_IDENTITY" = ambiguous ]; then
    printf 'claude-auth: fix REFUSED (the tmux server to seed could not be identified: %s) — seeding the server of the UID this process runs as would report success and leave the agent server untouched\n' \
      "$CLAUDE_AUTH_TMUX_IDENTITY_WHY"
    return 1
  fi
  if [ "$CLAUDE_AUTH_TMUX_IDENTITY" = delegate ]; then
    printf 'claude-auth: seeding the tmux server of the INVOKING agent (%s), not the one belonging to the UID this process runs as — bootstrap is documented to run under sudo\n' \
      "$CLAUDE_AUTH_TMUX_IDENTITY_USER"
  fi
  claude_auth_read_key_into __tok __state "$__file" "$CLAUDE_AUTH_TOKEN_KEY"
  CLAUDE_AUTH_SECRET="$__tok"
  if [ "$__state" != present ] || [ -z "$__tok" ]; then
    printf 'claude-auth: fix SKIPPED (%s holds no usable %s: %s) — there is nothing to seed FROM; provision it first\n' \
      "$__file" "$CLAUDE_AUTH_TOKEN_KEY" "$__state"
    return 1
  fi
  claude_auth_tmux_run "$CLAUDE_AUTH_TMUX_OP_BOUND" setenv -g "$CLAUDE_AUTH_TOKEN_KEY" "$__tok" 2>/dev/null
  rc=$?
  if [ "$rc" -ne 0 ]; then
    if claude_auth_bound_fired "$rc"; then
      printf 'claude-auth: fix FAILED (the tmux server did not accept `setenv -g %s` within %ss and the call was killed — the server is not answering; nothing was seeded)\n' \
        "$CLAUDE_AUTH_TOKEN_KEY" "$CLAUDE_AUTH_TMUX_OP_BOUND"
    else
      printf 'claude-auth: fix FAILED (tmux would not accept `setenv -g %s` — is a server running?)\n' "$CLAUDE_AUTH_TOKEN_KEY"
    fi
    return 1
  fi
  rc=0
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
  claude_auth_tmux_run "$CLAUDE_AUTH_TMUX_OP_BOUND" setenv -g "$CLAUDE_AUTH_CONFIG_KEY" "$__cfg" 2>/dev/null
  __cfgrc=$?
  if [ "$__cfgrc" -eq 0 ]; then
    printf 'claude-auth: seeded %s=%s into the running tmux server (source: %s)\n' \
      "$CLAUDE_AUTH_CONFIG_KEY" "$__cfg" "$__cfgsrc"
  elif claude_auth_bound_fired "$__cfgrc"; then
    # The same distinction the token half makes, for the same reason: "is a server running?"
    # is a wrong diagnosis for a server that IS running and is not answering.
    printf 'claude-auth: fix FAILED (the tmux server did not accept `setenv -g %s` within %ss and the call was killed — the server is not answering)\n' \
      "$CLAUDE_AUTH_CONFIG_KEY" "$CLAUDE_AUTH_TMUX_OP_BOUND"
    rc=1
  else
    printf 'claude-auth: fix FAILED (tmux would not accept `setenv -g %s`)\n' "$CLAUDE_AUTH_CONFIG_KEY"
    rc=1
  fi
  return $rc
}

# ---- the traced-run boundary: the PUBLIC names are the untraced wrappers ------------
# Each public entry point that reads or consumes the credential is defined HERE as a thin
# wrapper around its `__untraced` implementation, so no implementation has to remember to
# restore the flag on each of its dozen `return` paths — a per-return restore is a list to
# keep complete, and this repo has paid for that shape before.
# THE WRAPPED SET IS THE SET BOOTSTRAP AND THE CLI CAN REACH; every other helper in this
# file is called only from inside one of these, so it is already covered (and the depth
# counter makes a redundant wrap free if one is added later). That containment is asserted
# structurally by scripts/tests/test_claude_auth_capability.sh, against bootstrap's own
# source, rather than left as a claim here.
claude_auth_redact() {
  claude_auth_xtrace_off
  claude_auth_redact__untraced "$@"
  local __cax_rc=$?
  claude_auth_xtrace_restore
  return "$__cax_rc"
}
claude_auth_read_key_into() {
  claude_auth_xtrace_off
  claude_auth_read_key_into__untraced "$@"
  local __cax_rc=$?
  claude_auth_xtrace_restore
  return "$__cax_rc"
}
claude_auth_verdict_into() {
  claude_auth_xtrace_off
  claude_auth_verdict_into__untraced "$@"
  local __cax_rc=$?
  claude_auth_xtrace_restore
  return "$__cax_rc"
}
claude_tmux_env_verdict_into() {
  claude_auth_xtrace_off
  claude_tmux_env_verdict_into__untraced "$@"
  local __cax_rc=$?
  claude_auth_xtrace_restore
  return "$__cax_rc"
}
claude_auth_fix_tmux_env() {
  claude_auth_xtrace_off
  claude_auth_fix_tmux_env__untraced "$@"
  local __cax_rc=$?
  claude_auth_xtrace_restore
  return "$__cax_rc"
}
# WRAPPED THOUGH IT HANDLES NO SECRET, and deliberately so: bootstrap calls it, and the
# containment guard's rule is a SUBSET relation over derived sets, not a judgement about
# which callers are risky. An exemption for "this one is harmless" is one more list to keep
# complete, and the wrapper costs four lines.
claude_auth_emit_scope_note() {
  claude_auth_xtrace_off
  claude_auth_emit_scope_note__untraced "$@"
  local __cax_rc=$?
  claude_auth_xtrace_restore
  return "$__cax_rc"
}

# ---- CLI ---------------------------------------------------------------------------
claude_auth_usage() {
  printf 'usage: %s --auth [--show-probe-output] | --tmux-env | --report | --fix-tmux-env\n' "${0##*/}"
  printf '\n'
  printf 'THIS IS A REPORT, NOT A CHECK (issue #3733). Neither line certifies that this box\n'
  printf 'can start a lane, and THERE IS NO EXIT STATUS THAT SAYS SO: a report that was\n'
  printf 'PRINTED exits 0 whatever it found, so do not gate anything on the status. For the\n'
  printf 'three report modes the ONLY non-zero status is a usage error (2) — even a refused\n'
  printf 'test-only seam prints a line and exits 0. --fix-tmux-env is the exception, because\n'
  printf 'seeding is an ACTION. The FOUR things the observations CANNOT see are documented\n'
  printf 'in this file as LIMITATION 1..5 — five numbered slots, of which slot 4 is a RECORD\n'
  printf 'of one that was reclassified as a defect and FIXED rather than documented.\n'
  printf '\n'
  printf '  --auth          what happened when a `claude -p` run carrying the PERSISTED value\n'
  printf '                  was made? prints one `claude-auth:` line:\n'
  printf '                  PROBE-ANSWERED|NOT-PERSISTED|FAILED|UNMEASURED.\n'
  printf '                  Makes ONE real, HARD-bounded `claude -p` call (the bound escalates\n'
  printf '                  to SIGKILL; without one, no probe is run). PROBE-ANSWERED means rc 0\n'
  printf '                  AND the sentinel came back; it does NOT say the persisted value is\n'
  printf '                  what authenticated (LIMITATION 2), and the line names any alternate\n'
  printf '                  credential that was also in the environment.\n'
  printf '                  FAILED is reserved for a POSITIVELY IDENTIFIED rejection, because its\n'
  printf '                  remedy is "replace the value". A rate limit, an outage, a quota error,\n'
  printf '                  a crash or a missing sentinel are UNMEASURED with the cause named —\n'
  printf '                  never an instruction to discard a token.\n'
  printf '  --tmux-env      what does a tmux server hold, or what would a new one hand a pane?\n'
  printf '                  prints one `claude-tmux-env:` line: SERVER-CARRIES-BOTH|SERVER-STALE|\n'
  printf '                  SERVER-MISSING|SERVER-INCOMPLETE|SERVER-CONFIG-STALE|\n'
  printf '                  SERVER-CONFIG-NODIR when a server is running (its GLOBAL environment\n'
  printf '                  only — no pane is spawned, LIMITATION 5);\n'
  printf '                  COLD-START-DELIVERS-BOTH|COLD-START-MISSING|COLD-START-INCOMPLETE|\n'
  printf '                  COLD-START-NODIR when none is (an ISOLATED throwaway server on a\n'
  printf '                  private socket, handed the values read from /etc/environment —\n'
  printf '                  which observes tmux propagation, NOT pam_env delivery,\n'
  printf '                  LIMITATION 1); NO-SERVER|UNMEASURED when nothing could be read.\n'
  printf '  --report        both lines.\n'
  printf '  --fix-tmux-env  seed the RUNNING tmux server from the persisted value, then\n'
  printf '                  re-report. UNCONDITIONAL and OPERATOR-DRIVEN: nothing here can\n'
  printf '                  validate the value first, so this OVERWRITES whatever the server\n'
  printf '                  holds — which may be the only working credential on the box.\n'
  printf '                  Writes NO file; the token value is never printed. Its exit status\n'
  printf '                  IS about the seeding action, which is a thing that can fail.\n'
}

# claude_auth_emit_scope_note: printed by every report entry point, ONCE per invocation.
# A line that omits its own scope is indistinguishable from one that has none — the same
# reason the gate's narrowed lanes declare their narrowing at run time. It carries the
# `claude-auth-report:` prefix (distinct from both verdict-line prefixes) so it can neither
# be mistaken for an observation nor grepped away with one.
claude_auth_emit_scope_note__untraced() {
  printf 'claude-auth-report: OBSERVATIONS-ONLY — neither line above certifies that this box can start a lane (#3733). What they cannot see: pam_env DELIVERY (only tmux propagation), WHICH credential authenticated, whether the config dir is usable BY THE AGENT, and what a PANE receives as opposed to the server global table. See LIMITATION 1..5 in scripts/claude-auth-capability.sh (four live; slot 4 is a fixed-and-recorded one).\n'
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
  # NO VERDICT IS RETURNED. There is no passing state to return, and an exit status that
  # distinguished the states would BE the certification this file no longer makes — a
  # caller writing `if … --auth; then` is exactly how a proxy gets acted on. rc 0 means
  # "a report was printed".
  return 0
}

claude_auth_emit_tmux() {
  local v='' d=''
  claude_tmux_env_verdict_into v d
  printf 'claude-tmux-env: %s (%s)\n' "$v" "$(claude_auth_redact "$d")"
  # Same rule as claude_auth_emit_auth: the status is about the report, not the box.
  return 0
}

claude_auth_main() {
  local show=0 rc=0
  case "${1:-}" in
    # THE THREE REPORT MODES RETURN 0, UNCONDITIONALLY AND BY CONSTRUCTION. The `|| rc=1`
    # these arms used to carry is not merely dead now that the emitters always return 0 —
    # keeping it would be a place for a verdict to grow back, and it made the usage text
    # claim a non-zero status no input can produce. A refused test-only seam still PRINTS
    # its UNMEASURED line, so even that is a report.
    --auth)
      [ "${2:-}" = --show-probe-output ] && show=1
      claude_auth_emit_auth "$show"
      claude_auth_emit_scope_note
      return 0 ;;
    --tmux-env)     claude_auth_emit_tmux; claude_auth_emit_scope_note; return 0 ;;
    --report)
      claude_auth_emit_auth 0
      claude_auth_emit_tmux
      claude_auth_emit_scope_note
      return 0 ;;
    # THE ONE ENTRY POINT WHOSE STATUS IS STILL MEANINGFUL: seeding is an ACTION, and an
    # action that did not happen is a failure worth returning. The re-report that follows
    # it cannot change the status — it is a report.
    --fix-tmux-env) claude_auth_fix_tmux_env || rc=1; claude_auth_emit_tmux; claude_auth_emit_scope_note; return $rc ;;
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
