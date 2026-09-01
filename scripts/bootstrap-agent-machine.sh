#!/usr/bin/env bash
# bootstrap-agent-machine.sh (issue #1921)
#
# One-command bootstrap for a FRESH machine that will run CQLite agent work.
# Idempotent: safe to re-run any time. macOS + Linux. It CHECKS everything the
# throughput-sprint setup depends on and either installs (only with --yes) or
# prints the exact install command; it NEVER installs silently.
#
# What it verifies:
#   1. Rust toolchain (cargo) — needed to build + to `cargo install` on Linux.
#   2. Accelerators the gate auto-detects (issue #1848): sccache, cargo-nextest,
#      modern bash (>=4.3 for parallel component lanes), and — on Linux only —
#      the mold linker (issue #2859), wired via a managed block in the per-machine
#      ~/.cargo/config.toml. Detection here MIRRORS the gate's ACCEL_* block
#      (scripts/agent-gate.sh, "sccache auto-detect" / "cargo-nextest auto-detect"
#      / "ACCEL_LANES" / "mold link-accelerator") so the two can never disagree
#      about whether an accelerator is present. The final health check below
#      re-reads the state straight from the gate to reconcile.
#      2c's functional check additionally reports WHOSE capability it measured: root
#      BYPASSES perf_event_paranoid, so under `sudo bootstrap` the probe drops privilege
#      (setpriv/runuser/sudo -u) and, where it cannot, says the result is not evidence
#      about an unprivileged agent process. "VERIFIED" needs BOTH /proc=ok and that pass.
#   2c. Perf profiling capability (issue #3249, LINUX only): the reboot-surviving
#      /etc/sysctl.d/99-cqlite-perf.conf drop-in (kernel.perf_event_paranoid = -1,
#      kernel.kptr_restrict = 0), applied now, READ BACK out of /proc — a `sysctl`
#      write's return code proves nothing — and then FUNCTIONALLY verified by running
#      the collection the measurement doctrine mandates (`perf stat -C 0 -e cycles`),
#      requiring exit 0 AND a non-zero cycle count. Images ship paranoid=4, which
#      denies ALL unprivileged perf use: a PERMISSION verdict whose "access limited"
#      help text reads like a missing CAPABILITY. Advisory like mold — no sudo, no
#      perf, or an absent /proc control degrades to a `[warn]` plus the exact
#      write-AND-apply remedy line, and the run still exits 0.
#   3. gh auth + BOARD ACCESS (Path A board dispatch, #1886). The verdict comes
#      from a READ-ONLY functional probe of the board, never from the `project`
#      scope string (issue #2942): a token can carry `project` and still fail
#      `gh project item-edit` for a missing `read:org` while the equivalent
#      `updateProjectV2ItemFieldValue` GraphQL mutation works. Scopes are read from
#      the ACTIVE account's stanza only, and the probe runs as the account flow-board
#      forces active, restoring the operator's account afterwards. Overridable with
#      CQLITE_PROJECT_OWNER / CQLITE_PROJECT_NUMBER / CQLITE_PROJECT_ACCOUNT.
#   3b. git push CREDENTIALS (issue #2942) — separate from `gh` auth. The claim
#      protocol (scripts/flow/claim.sh, claim-heartbeat.sh) pushes with plain git
#      on 10+ call sites, so an authenticated gh with an unauthenticated git means
#      the cross-machine lock does not work. Under --yes (or --fix-credentials) this
#      configures a credential path scoped to the origin host, preferring
#      `gh auth setup-git`, else a helper that dereferences $GH_TOKEN at call time.
#      The token is never written to disk.
#      It then measures git PUSH CAPABILITY (issue #3369) by performing THE
#      OPERATION — `scripts/flow/claim.sh smoke` creates, reads back and deletes a
#      throwaway refs/claims/smoke-<commit-sha> ref on the origin — because every check
#      before it is evidence about CONFIGURATION, not about the operation: the box
#      that motivated #3369 passed `gh auth status` AND `git ls-remote origin HEAD`
#      and still failed every claim push. The verdict is THREE-valued and prints one
#      greppable `git-push:` line — VERIFIED (affirmatively measured), FAILED, or
#      UNMEASURED (no remote/unreachable/no bound available). UNMEASURED is a [warn],
#      never an [ok]: an unmeasured capability must not inherit the permissive branch.
#   4. roborev installed and its LOCAL config resolves — roborev follows THIS
#      machine's configured agent (commonly codex via .roborev.toml); we warn
#      only if the local config is broken, never prescribe an agent.
#   5. Datasets present + CQLITE_DATASETS_ROOT guidance.
#   5b. Single-gate pin (issues #2640/#3414). Persists CQLITE_GATE_MAX_CONCURRENCY=1
#      into /etc/environment — which PAM reads at SESSION CREATION, with no
#      interactivity guard — and takes its VERDICT from an AFFIRMATIVE PROBE of a
#      fresh, profile-free session, never from a grep of the file it just wrote.
#      VISIBILITY IS ONLY HALF THE QUESTION: a value the gate does not HONOUR is a pin
#      in name only, so the gate itself is then asked (via its `--cpu-budget` hook,
#      never a re-derivation of its rules) what it will do with the value. One
#      greppable `gate-pin:` line: VERIFIED / NOT-SYSTEM-WIDE / NOT-HONOURED / FAILED /
#      UNMEASURED, and only VERIFIED is an [ok] (same posture as `git-push:`). VERIFIED
#      requires BOTH halves — the line present in the system-wide file AND a fresh
#      session that sees a value the gate honours — because file-only was the original
#      #3414 defect and session-only certifies a pin that may reach sudo sessions alone,
#      and the file's VALUE must EQUAL the session's (presence alone let a file saying
#      `abc` pass while a per-user override supplied `1`). NON-LINUX hosts get UNMEASURED
#      and never a success: with no PAM-read system-wide file there is nothing to correlate
#      the session value against, so a machine-wide pin cannot be told apart from a sudo-
#      or user-scoped one that ordinary gate processes never see. No verdict that reports a
#      state is available there, so none is given. `verify.run` runs on Linux, so nothing
#      on this fleet regresses. `--yes` persists it, and
#      so does the narrow `--fix-gate-pin` that `.agent-ami/profile.yaml`'s verify.run
#      passes, so a freshly launched box is PINNED rather than merely reported unpinned.
#      PAM reads /etc/environment at session creation, so the probe in the SAME run sees
#      what the write just persisted — no reboot and no re-login.
#   5b2. sccache cache-size cap (issue #3727). The SAME defect as 5b, one variable over:
#      `.agent-ami/profile.yaml` DECLARES SCCACHE_CACHE_SIZE but nothing ever persisted it, so
#      the value existed only inside launcher-created processes and the fleet-effective cap was
#      sccache's own 10 GiB default. Persists SCCACHE_CACHE_SIZE into /etc/environment (never
#      rewriting an existing value) and takes its VERDICT from a fresh profile-free session
#      PLUS the RUNNING SERVER — because the cap is read by the sccache SERVER at startup and
#      is therefore fixed by whichever process started it: a visible env var proves nothing
#      once a server is already up. One greppable `sccache-cap:` line: VERIFIED /
#      NOT-SYSTEM-WIDE / NOT-HONOURED / FAILED / UNMEASURED, and only VERIFIED is an [ok].
#      NOT-HONOURED's remedy is `sccache --stop-server`, NOT editing the value. The value->bytes
#      map is asked of an ISOLATED throwaway sccache server, never reimplemented here: measured,
#      `30G` is 30 GiB but `30GiB` and `30GB` are SILENTLY DISCARDED to the 10 GiB default and a
#      bare integer means BYTES, so a bash reimplementation is exactly where this goes wrong.
#      Non-Linux hosts get UNMEASURED and never a success (same posture as 5b).
#   6. Health check: run the gate's fmt component and print its authoritative
#      `accelerators:` line.
#
# Usage:
#   bash scripts/bootstrap-agent-machine.sh            # check + print install cmds
#   bash scripts/bootstrap-agent-machine.sh --yes      # also auto-run installs + git credentials
#   bash scripts/bootstrap-agent-machine.sh --fix-credentials  # wire git push credentials ONLY
#                                                      #   (section 3b's auto-fix, the same
#                                                      #   `gh auth setup-git`-preferring path
#                                                      #   --yes uses); every other check stays
#                                                      #   read-only — no toolchain installs.
#   bash scripts/bootstrap-agent-machine.sh --strict   # exit 1 when there is any [warn]
#                                                      #   (default stays exit 0 so this script
#                                                      #   still composes into setup scripts)
#   bash scripts/bootstrap-agent-machine.sh --skip-push-probe  # skip ONLY section 3b's push
#                                                      #   probe (the refs/claims/* smoke push).
#                                                      #   Loud + non-passing: it emits
#                                                      #   `git-push: OPT-OUT` as a [warn], so it
#                                                      #   withholds "All checks green." and can
#                                                      #   never buy a vacuous green. For offline
#                                                      #   boxes and hermetic self-tests.
#   bash scripts/bootstrap-agent-machine.sh --fix-gate-pin     # persist the single-gate pin
#                                                      #   (section 5b's /etc/environment write)
#                                                      #   WITHOUT --yes, and nothing else — the
#                                                      #   sibling of --fix-credentials, and what
#                                                      #   .agent-ami/profile.yaml's verify.run
#                                                      #   uses so a freshly launched box is
#                                                      #   PINNED rather than merely reported
#                                                      #   unpinned. Needs privilege; when it
#                                                      #   cannot persist, the section stays
#                                                      #   non-passing (never an [ok]).
#   bash scripts/bootstrap-agent-machine.sh --skip-gate-pin    # skip ONLY section 5b (the
#                                                      #   single-gate pin: the /etc/environment
#                                                      #   write AND the PAM-session visibility
#                                                      #   probe). Same posture as
#                                                      #   --skip-push-probe: it emits
#                                                      #   `gate-pin: OPT-OUT` as a [warn], so it
#                                                      #   can never buy a vacuous green.
#                                                      #   CQLITE_BOOTSTRAP_SKIP_GATE_PIN=1 is the
#                                                      #   env spelling, for harnesses that drive
#                                                      #   bootstrap on a fixed command line.
#   bash scripts/bootstrap-agent-machine.sh --fix-sccache-cap  # persist the sccache cache-size
#                                                      #   cap (section 5b2's /etc/environment
#                                                      #   write) WITHOUT --yes, and nothing
#                                                      #   else — the sibling of
#                                                      #   --fix-credentials / --fix-gate-pin,
#                                                      #   and what .agent-ami/profile.yaml's
#                                                      #   verify.run uses. One flag per
#                                                      #   subject: a cache-size cap is not a
#                                                      #   gate-slot cap (issue #3727).
#   bash scripts/bootstrap-agent-machine.sh --skip-sccache-cap # skip ONLY section 5b2. Same
#                                                      #   posture as --skip-gate-pin: it emits
#                                                      #   `sccache-cap: OPT-OUT` as a [warn],
#                                                      #   so it can never buy a vacuous green.
#                                                      #   CQLITE_BOOTSTRAP_SKIP_SCCACHE_CAP=1
#                                                      #   is the env spelling.
#   bash scripts/bootstrap-agent-machine.sh --skip-smoke   # skip the final GATE run (section 6).
#                                                      #   DISTINCT from --skip-push-probe: this
#                                                      #   one is about the gate fmt smoke, that
#                                                      #   one about the git push probe.
#   bash scripts/bootstrap-agent-machine.sh --help
set -uo pipefail

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
GATE="$REPO_ROOT/scripts/agent-gate.sh"

AUTO_YES=0
SKIP_SMOKE=0
# --skip-push-probe is deliberately NOT spelled --skip-smoke-push or folded into
# --skip-smoke: --skip-smoke skips the GATE fmt run in section 6 and has nothing to do
# with git. Two different subjects, two different flags (issue #3369).
SKIP_PUSH_PROBE=0
# --skip-gate-pin / CQLITE_BOOTSTRAP_SKIP_GATE_PIN=1 (issue #3414): skip section 5b
# entirely — no /etc/environment write and no PAM session probe. A THIRD subject, so a
# third switch, exactly as --skip-push-probe is separate from --skip-smoke. It is LOUD
# and NON-PASSING (a `gate-pin: OPT-OUT` [warn]), so it withholds "All checks green."
# and --strict still exits 1: an opt-out that returned `ok` would be a switch for buying
# a vacuous green, which is the failure mode this whole section exists to remove. The
# env spelling exists because the sibling self-suites drive bootstrap on fixed command
# lines they cannot easily grow an argument on; both spellings take the same code path
# and print the same non-passing verdict, and the message names which one was used.
SKIP_GATE_PIN=0
SKIP_GATE_PIN_HOW=""
if [ "${CQLITE_BOOTSTRAP_SKIP_GATE_PIN:-0}" = 1 ]; then
  SKIP_GATE_PIN=1; SKIP_GATE_PIN_HOW="CQLITE_BOOTSTRAP_SKIP_GATE_PIN=1"
fi
# --fix-gate-pin (issue #3414): perform section 5b's /etc/environment write WITHOUT --yes,
# and nothing else. A SIBLING of --fix-credentials, deliberately not a widening of it:
# that flag documents itself as running section 3b's auto-fix "and NOTHING else", and
# quietly making that false is the drift this codebase pays for later. Same one-flag-per-
# subject rule --skip-push-probe / --skip-smoke / --skip-gate-pin already follow.
#
# WHY A REPAIR FLAG AT ALL. `.agent-ami/profile.yaml`'s verify.run is the only bootstrap
# invocation a launcher-onboarded box ever gets, and it does not pass --yes. Without this
# flag a fresh box is left UNPINNED and verify merely reds — which converts #3414's silent
# defect into a loud one without removing it, and a verify that reds on every new box is an
# alarm people learn to waive. Persisting env wiring is not a toolchain install, so verify
# stays a verification step rather than becoming an installer — the same reasoning that
# justified --fix-credentials.
FIX_GATE_PIN=0
# --fix-sccache-cap / --skip-sccache-cap (issue #3727): section 5b2's own switches. ONE FLAG PER
# SUBJECT, the rule --skip-push-probe / --skip-smoke / --skip-gate-pin already follow — a cache-size
# cap and a gate-slot cap are different variables with different consumers and different remedies
# (`sccache --stop-server` vs editing a value), so folding this into --fix-gate-pin would make that
# flag's own "and NOTHING else" documentation false, which is the drift this file pays for later.
FIX_SCCACHE_CAP=0
SKIP_SCCACHE_CAP=0
SKIP_SCCACHE_CAP_HOW=""
if [ "${CQLITE_BOOTSTRAP_SKIP_SCCACHE_CAP:-0}" = 1 ]; then
  SKIP_SCCACHE_CAP=1; SKIP_SCCACHE_CAP_HOW="CQLITE_BOOTSTRAP_SKIP_SCCACHE_CAP=1"
fi
FIX_CREDENTIALS=0
STRICT=0
for arg in "$@"; do
  case "$arg" in
    --yes|-y) AUTO_YES=1 ;;
    --skip-smoke) SKIP_SMOKE=1 ;;
    --skip-push-probe) SKIP_PUSH_PROBE=1 ;;
    --skip-gate-pin) SKIP_GATE_PIN=1; SKIP_GATE_PIN_HOW="--skip-gate-pin" ;;
    --fix-gate-pin) FIX_GATE_PIN=1 ;;
    --skip-sccache-cap) SKIP_SCCACHE_CAP=1; SKIP_SCCACHE_CAP_HOW="--skip-sccache-cap" ;;
    --fix-sccache-cap) FIX_SCCACHE_CAP=1 ;;
    --fix-credentials) FIX_CREDENTIALS=1 ;;
    --strict) STRICT=1 ;;
    -h|--help)
      # Print the whole leading comment block, bounded by the FIRST `set -` line
      # rather than a hardcoded line number: a fixed `2,45p` silently truncates the
      # help the moment the header grows (it already omitted the perf section).
      awk 'NR == 1 { next } /^set -/ { exit } { sub(/^# ?/, ""); print }' "${BASH_SOURCE[0]}"
      exit 0 ;;
    *) echo "bootstrap: unknown arg: $arg (try --help)" >&2; exit 2 ;;
  esac
done

# Contradictory intents must not resolve silently — "I told it to fix the pin and it
# skipped the section" is exactly the class of quiet surprise this issue is about.
# An EXPLICIT --skip-gate-pin beside --fix-gate-pin is a usage error; the ENV opt-out is
# the weaker signal, so an explicit --fix-gate-pin overrides it (a harness exporting
# CQLITE_BOOTSTRAP_SKIP_GATE_PIN=1 must not be able to neuter a caller's explicit repair).
# Resolved here, after the loop, so flag ORDER cannot change the outcome.
if [ "$FIX_GATE_PIN" = 1 ] && [ "$SKIP_GATE_PIN" = 1 ]; then
  if [ "$SKIP_GATE_PIN_HOW" = "--skip-gate-pin" ]; then
    echo "bootstrap: --skip-gate-pin and --fix-gate-pin are contradictory (try --help)" >&2
    exit 2
  fi
  SKIP_GATE_PIN=0; SKIP_GATE_PIN_HOW=""
fi

# Same rule, same shape, for 5b2's pair (issue #3727). Resolved after the loop so flag ORDER cannot
# change the outcome, and an EXPLICIT skip beside an explicit fix is a usage error while the weaker
# ENV opt-out loses to an explicit --fix-sccache-cap.
if [ "$FIX_SCCACHE_CAP" = 1 ] && [ "$SKIP_SCCACHE_CAP" = 1 ]; then
  if [ "$SKIP_SCCACHE_CAP_HOW" = "--skip-sccache-cap" ]; then
    echo "bootstrap: --skip-sccache-cap and --fix-sccache-cap are contradictory (try --help)" >&2
    exit 2
  fi
  SKIP_SCCACHE_CAP=0; SKIP_SCCACHE_CAP_HOW=""
fi

# ---- OS + package-manager detection ----
OS=$(uname -s)
case "$OS" in
  Darwin) PLATFORM=macos ;;
  Linux)  PLATFORM=linux ;;
  *)      PLATFORM=other ;;
esac

WARNINGS=0
have() { command -v "$1" >/dev/null 2>&1; }

# ---- bounded execution (issue #2942) ----
# GNU coreutils installs its timeout as `gtimeout` on stock macOS, so resolving only
# `timeout` leaves every bound below INERT on macOS — which is the fleet's platform and
# the one where two of the three hang scenarios live (a locked osxkeychain, a Git
# Credential Manager browser flow). Resolve both, and degrade visibly (unbounded) only
# when neither exists.
# Resolution PROBES the candidate rather than trusting it (#3369 review). --kill-after is
# a GNU coreutils flag: BusyBox and older implementations REJECT it, and a non-GNU
# `timeout` earlier on PATH than a GNU `gtimeout` would be selected by a
# first-match-wins lookup. A selected binary that rejects the flag makes EVERY bounded
# call fail — board access, credential probe, push probe — so --strict would then reject
# a perfectly healthy machine: the bound-hardening would have inverted this change's
# purpose. So each candidate is tried WITH the flag; a candidate that supports it wins
# even if it is second, and a candidate that does not is still usable with the bound
# degraded to SIGTERM-only (the pre-hardening behaviour, and far better than every
# bounded call failing). The probe asks about BEHAVIOUR; nothing here sniffs a vendor.
TIMEOUT_BIN=""
TIMEOUT_KILL_AFTER=0
for _tb_name in timeout gtimeout; do
  _tb_path="$(command -v "$_tb_name" 2>/dev/null || true)"
  [ -n "$_tb_path" ] || continue
  if "$_tb_path" --kill-after=1 1 true >/dev/null 2>&1; then
    TIMEOUT_BIN="$_tb_path"; TIMEOUT_KILL_AFTER=1; break
  fi
  # Usable, but without the escalation. Keep it as the fallback and keep looking.
  [ -n "$TIMEOUT_BIN" ] || TIMEOUT_BIN="$_tb_path"
done
unset _tb_name _tb_path
# bounded <secs> <cmd...> — run <cmd...> under the resolved timeout binary if there is
# one, else run it directly. Use `env VAR=... cmd` when the call needs env prefixes.
#
# --kill-after IS THE BOUND (#3369 review). Plain `timeout <secs>` sends SIGTERM and then
# WAITS: a child that traps or ignores SIGTERM — git, ssh, a Git Credential Manager, a
# credential helper — runs on indefinitely, so the advertised bound bounds nothing.
# Measured: `timeout 3` on a TERM-ignoring child returned after 30s (rc 124); with
# `--kill-after=2` it returned after 5s (rc 137). This is boot-path code where a hang is
# the worst outcome, and this change newly routes a NETWORK PUSH through here. 5s is
# ample grace for a well-behaved child to finish its own cleanup. Both `timeout` and
# `gtimeout` accept the flag, and the degrade-visibly-when-neither-exists path below is
# unchanged.
BOUNDED_KILL_GRACE=5
bounded() {
  local secs="$1"; shift
  if [ -n "$TIMEOUT_BIN" ] && [ "$TIMEOUT_KILL_AFTER" = 1 ]; then
    "$TIMEOUT_BIN" --kill-after="$BOUNDED_KILL_GRACE" "$secs" "$@"
  elif [ -n "$TIMEOUT_BIN" ]; then
    "$TIMEOUT_BIN" "$secs" "$@"
  else
    "$@"
  fi
}
ok()   { printf '  \033[32m[ok]\033[0m   %s\n' "$1"; }
warn() { printf '  \033[33m[warn]\033[0m %s\n' "$1"; WARNINGS=$((WARNINGS + 1)); }
info() { printf '         %s\n' "$1"; }
hdr()  { printf '\n\033[1m== %s ==\033[0m\n' "$1"; }

# run_or_print <label> <install-cmd...>: run it under --yes, else print it.
run_or_print() {
  local label="$1"; shift
  if [ "$AUTO_YES" = 1 ]; then
    info "installing $label: $*"
    if "$@"; then ok "$label installed"; else warn "$label install FAILED — run manually: $*"; fi
  else
    info "install $label:  $*"
    info "(re-run with --yes to auto-install, or run the line above)"
  fi
}

# macOS install goes through brew; Linux accelerators install via cargo.
brew_or_cargo() {
  # brew_or_cargo <brew-formula> <cargo-crate>
  if [ "$PLATFORM" = macos ] && have brew; then
    echo "brew install $1"
  else
    echo "cargo install $2"
  fi
}

# ---- mold link accelerator helpers (Linux only, issue #2859) ----
# mold is the fast Linux linker; linking is the one build cost sccache cannot
# cache, so every --lite round and full gate re-links every test binary from
# scratch. These helpers detect/install mold, prove the toolchain accepts it via a
# link probe, and wire it through a delimited managed block in the PER-MACHINE
# ~/.cargo/config.toml (honoring $CARGO_HOME). They NEVER touch the repo-committed
# .cargo/config.toml, are idempotent (replace-the-block), and preserve all content
# outside the markers. All of this is inert on non-Linux hosts (gated by the caller).
MOLD_BEGIN='# BEGIN cqlite-mold (managed by scripts/bootstrap-agent-machine.sh — do not edit inside)'
MOLD_END='# END cqlite-mold'

# mold_link_probe <compiler>: true iff <compiler> links a trivial program with
# -fuse-ld=mold (proves the toolchain will not break linking before we write config).
mold_link_probe() {
  local cc="$1"
  have "$cc" || return 1
  local d
  d=$(mktemp -d 2>/dev/null) || return 1
  printf 'int main(void){return 0;}\n' >"$d/probe.c"
  local rc=0
  "$cc" -fuse-ld=mold "$d/probe.c" -o "$d/probe" >/dev/null 2>&1 || rc=1
  rm -rf "$d"
  return $rc
}

# mold_target_section <triple> <linker>: emit one cargo [target.<triple>] section
# routing linking through mold; adds `linker = "<linker>"` only when non-empty.
mold_target_section() {
  local triple="$1" linker="$2"
  printf '[target.%s]\n' "$triple"
  [ -n "$linker" ] && printf 'linker = "%s"\n' "$linker"
  printf 'rustflags = ["-C", "link-arg=-fuse-ld=mold"]\n'
}

# mold_write_block <linker>: (re)write the managed block in the per-machine cargo
# config. Strips any prior block (and one blank line immediately preceding it) so
# re-runs are byte-idempotent, preserves everything else, then appends the block
# with both Linux target triples. Writes into whichever config file cargo actually
# reads — the extension-less `config` WINS when both exist (a documented legacy
# precedence cargo warns about), else `config.toml` — so a machine that only has
# the legacy `~/.cargo/config` never gets a shadow file that cargo would ignore,
# and a both-files machine never has the block land in the ignored file.
mold_write_block() {
  local linker="$1"
  local cfg_dir cfg_file preserved
  cfg_dir="${CARGO_HOME:-$HOME/.cargo}"
  if ! mkdir -p "$cfg_dir" 2>/dev/null; then
    warn "could not create $cfg_dir — skipping mold linker config"
    return 0
  fi
  if [ -f "$cfg_dir/config" ]; then
    cfg_file="$cfg_dir/config"
  elif [ -f "$cfg_dir/config.toml" ]; then
    cfg_file="$cfg_dir/config.toml"
  else
    cfg_file="$cfg_dir/config.toml"
  fi
  preserved=$(mktemp) || { warn "mktemp failed — skipping mold linker config"; return 0; }
  if [ -f "$cfg_file" ]; then
    awk -v b="$MOLD_BEGIN" -v e="$MOLD_END" '
      { lines[NR] = $0 }
      END {
        start = 0
        for (i = 1; i <= NR; i++) if (lines[i] == b) { start = i; break }
        if (start == 0) { for (i = 1; i <= NR; i++) print lines[i]; exit }
        endi = 0
        for (i = start; i <= NR; i++) if (lines[i] == e) { endi = i; break }
        if (endi == 0) endi = NR
        rmstart = start
        if (start > 1 && lines[start-1] == "") rmstart = start - 1
        for (i = 1; i <= NR; i++) if (i < rmstart || i > endi) print lines[i]
      }
    ' "$cfg_file" >"$preserved"
  else
    : >"$preserved"
  fi
  # Fail-safe #1: a user-defined [target.<triple>-unknown-linux-gnu] section OUTSIDE
  # our markers would collide with the block we append (TOML table redefinition =
  # cargo parse error on EVERY invocation). Never risk it — warn and write nothing,
  # leaving the file byte-identical.
  if grep -Eq '^\[target\.(x86_64|aarch64)-unknown-linux-gnu\]' "$preserved"; then
    warn "existing [target.<triple>-unknown-linux-gnu] section in $cfg_file — writing NO mold block (a second table would be a cargo parse error); add \"-C link-arg=-fuse-ld=mold\" to that section by hand, or remove it and re-run bootstrap"
    rm -f "$preserved"
    return 0
  fi
  # Fail-safe #2: a pre-existing [build] rustflags (or a dotted build.rustflags) is
  # first-match-wins over our target.rustflags — writing the block would SILENTLY
  # disable the user's global flags. Same posture: warn and write nothing.
  if awk '
      /^\[build\]/ { inbuild = 1; next }
      /^\[/        { inbuild = 0 }
      inbuild && /^[[:space:]]*rustflags[[:space:]]*=/ { found = 1 }
      /^[[:space:]]*build\.rustflags[[:space:]]*=/     { found = 1 }
      END { exit(found ? 0 : 1) }
    ' "$preserved"; then
    warn "existing [build] rustflags in $cfg_file — writing NO mold block (target.rustflags would silently disable the user's build rustflags); add \"-C link-arg=-fuse-ld=mold\" to that rustflags list by hand, or remove it and re-run bootstrap"
    rm -f "$preserved"
    return 0
  fi
  # Atomic write: build the new content in a temp file in the SAME directory, then
  # rename over the target — so an ENOSPC/interrupt mid-write can never leave a
  # truncated config (which would break every cargo invocation). Resolve a symlink
  # to its target so we never silently replace a symlinked config with a plain file.
  local write_target="$cfg_file" tmpw
  if [ -L "$cfg_file" ]; then
    write_target=$(readlink -f "$cfg_file" 2>/dev/null || echo "$cfg_file")
  fi
  tmpw=$(mktemp "$(dirname "$write_target")/.cqlite-mold.XXXXXX" 2>/dev/null) \
    || { warn "mktemp failed in $(dirname "$write_target") — skipping mold linker config"; rm -f "$preserved"; return 0; }
  {
    cat "$preserved"
    [ -s "$preserved" ] && printf '\n'
    printf '%s\n' "$MOLD_BEGIN"
    mold_target_section "x86_64-unknown-linux-gnu" "$linker"
    printf '\n'
    mold_target_section "aarch64-unknown-linux-gnu" "$linker"
    printf '%s\n' "$MOLD_END"
  } >"$tmpw"
  rm -f "$preserved"
  if mv -f "$tmpw" "$write_target"; then
    ok "wrote mold managed block to $cfg_file (both Linux target triples${linker:+, linker=$linker})"
  else
    warn "could not install mold config at $write_target — original left intact"
    rm -f "$tmpw"
  fi
}

# mold_configure_linux: link-probe cc (then clang) and, on success, wire mold via
# the managed block. Fail-safe: if no compiler accepts -fuse-ld=mold, WARN and
# write NOTHING (a machine must never end up with a config that breaks linking).
mold_configure_linux() {
  local linker=""
  if mold_link_probe cc; then
    ok "link probe passed (cc accepts -fuse-ld=mold)"
  elif mold_link_probe clang; then
    linker="clang"
    ok 'link probe passed (clang accepts -fuse-ld=mold; managed block sets linker = "clang")'
  else
    warn "link probe FAILED — no C compiler accepts -fuse-ld=mold; writing NO linker config (fail-safe)"
    return 0
  fi
  mold_write_block "$linker"
}

echo "CQLite agent-machine bootstrap (issue #1921) — platform: $PLATFORM"

# ---- 1. Rust toolchain ----
hdr "Rust toolchain"
if have cargo; then
  ok "cargo present ($(cargo --version 2>/dev/null))"
else
  warn "cargo NOT found — install Rust first: https://rustup.rs (curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh)"
fi

# ---- 2. Accelerators (mirror scripts/agent-gate.sh ACCEL_* detection, #1848) ----
hdr "Gate accelerators (issue #1848)"

# sccache — mirrors agent-gate.sh "sccache auto-detect" (ACCEL_SCCACHE).
if have sccache; then
  ok "sccache present ($(sccache --version 2>/dev/null | head -1)) — cross-worktree compile cache"
else
  warn "sccache MISSING — ~25.6% slower fresh builds (gate stamps sccache=absent)"
  # shellcheck disable=SC2046
  run_or_print sccache $(brew_or_cargo sccache sccache)
fi

# cargo-nextest — mirrors agent-gate.sh "cargo-nextest auto-detect" (ACCEL_NEXTEST).
if have cargo-nextest; then
  ok "cargo-nextest present ($(cargo nextest --version 2>/dev/null | head -1)) — parallel core-tests"
else
  warn "cargo-nextest MISSING — core-tests fall back to serial cargo test (gate stamps nextest=absent)"
  # shellcheck disable=SC2046
  run_or_print cargo-nextest $(brew_or_cargo cargo-nextest cargo-nextest)
fi

# modern bash — mirrors agent-gate.sh ACCEL_LANES (needs bash >=4.3 for `wait -n`).
# Iterate ALL candidate installs and track the NEWEST version found. The gate,
# however, runs under whatever plain `bash` resolves to on PATH — so a machine
# that HAS a modern bash but bad PATH order needs a PATH fix, not an install,
# and the gate's lanes WARN persists until PATH is fixed.
bash_ver_of() { "$1" -c 'echo "${BASH_VERSINFO[0]}.${BASH_VERSINFO[1]}"' 2>/dev/null; }
ver_num() { local v="${1:-0.0}"; echo $(( ${v%%.*} * 100 + ${v#*.} )); }
BEST_BV="0.0"; BEST_BV_PATH=""
for b in bash /opt/homebrew/bin/bash /usr/local/bin/bash; do
  p=$(command -v "$b" 2>/dev/null) || continue
  v=$(bash_ver_of "$p") || continue
  if [ "$(ver_num "$v")" -gt "$(ver_num "$BEST_BV")" ]; then
    BEST_BV="$v"; BEST_BV_PATH="$p"
  fi
done
PLAIN_BASH=$(command -v bash 2>/dev/null || true)
PLAIN_BV="0.0"
[ -n "$PLAIN_BASH" ] && PLAIN_BV=$(bash_ver_of "$PLAIN_BASH" || echo "0.0")
if [ "$(ver_num "$PLAIN_BV")" -ge "$(ver_num 4.3)" ]; then
  ok "modern bash present (bash $PLAIN_BV at $PLAIN_BASH) — parallel gate component lanes"
elif [ "$(ver_num "$BEST_BV")" -ge "$(ver_num 4.3)" ]; then
  # A modern bash IS installed, but plain `bash` resolves to an old one — the
  # gate sees whatever `bash` resolves to, so this is a PATH-order problem.
  warn "modern bash installed (bash $BEST_BV at $BEST_BV_PATH) but plain 'bash' resolves to ${PLAIN_BASH:-<none>} (bash $PLAIN_BV) — gate lanes run SERIALLY (lanes=serial)"
  info "fix PATH order, no install needed: put $(dirname "$BEST_BV_PATH") ahead of /bin in PATH"
  info "note: the gate sees whatever 'bash' resolves to — its lanes WARN persists until PATH is fixed"
else
  warn "bash <4.3 (newest found: $BEST_BV) — gate lanes run SERIALLY (gate stamps lanes=serial)"
  if [ "$PLATFORM" = macos ]; then
    # shellcheck disable=SC2046
    run_or_print bash $(brew_or_cargo bash bash)
    info "note: macOS ships bash 3.2; brew's bash lands on PATH ahead of /bin/bash"
  else
    info "install a newer bash via your distro package manager (apt/dnf/pacman)"
  fi
fi

# ---- 2b. Link accelerator: mold (Linux only, issue #2859) ----
# Advisory, mirroring the sccache/nextest ok/warn pattern: a missing or
# uninstallable mold never fails the run. macOS is out of scope (mold is
# Linux-only; Apple's ld-prime is already the fastest linker there) — this whole
# section is skipped on Darwin, so Darwin output is byte-identical to pre-change.
if [ "$PLATFORM" = linux ]; then
  hdr "Link accelerator: mold (Linux, issue #2859)"
  MOLD_COST="linking is the one build cost sccache cannot cache — every --lite round and full gate re-links every test binary from scratch (GNU bfd/lld are materially slower, especially on aarch64/Graviton)"
  if have mold; then
    ok "mold present ($(mold --version 2>/dev/null | head -1)) — fast Linux linker"
    # Probe the toolchain, then wire mold via the per-machine cargo config block.
    mold_configure_linux
  else
    warn "mold MISSING — $MOLD_COST"
    if have apt-get; then
      run_or_print mold sudo apt-get install -y mold
    elif have apt; then
      run_or_print mold sudo apt install -y mold
    elif have dnf; then
      run_or_print mold sudo dnf install -y mold
    elif have yum; then
      run_or_print mold sudo yum install -y mold
    elif have pacman; then
      run_or_print mold sudo pacman -S --noconfirm mold
    else
      warn "no supported package manager (apt/dnf/yum/pacman) found — install mold manually"
    fi
    # Under --yes the install above may have just placed mold on PATH; wire it NOW
    # (probe + managed block) so one --yes run delivers the FULL acceleration. In
    # print-only mode nothing was installed, so keep the re-run hint instead.
    if [ "$AUTO_YES" = 1 ] && have mold; then
      ok "mold now present ($(mold --version 2>/dev/null | head -1)) — configuring linker"
      mold_configure_linux
    else
      info "linker config is written only after mold is installed AND a link probe passes — re-run bootstrap once mold is on PATH"
    fi
  fi
fi

# ---- 2c. Perf profiling capability (issue #3249) ----------------------------
# Agent/worker images ship with kernel.perf_event_paranoid = 4 and set it NOWHERE
# in /etc/sysctl.conf or /etc/sysctl.d, so every profiling run starts from a hard
# EACCES whose help text ("access limited") reads like a CAPABILITY verdict when it
# is a PERMISSION verdict — two measurement cycles were lost to that. A box left
# permissive by a hand-probe is no better: with no drop-in, it reverts on
# reboot/reprovision, i.e. the fleet is profileable only by accident.
#
# Posture mirrors mold above: PROBE FIRST, write only what is needed, and never
# fail the run — a box without sudo or without perf degrades to a warn plus the
# exact remedy line. What is NOT advisory is the VERDICT: a bootstrap that
# silently leaves a box unprofileable is the failure mode being fixed, so the
# section ends with a FUNCTIONAL `perf stat -C 0` verification (AC2), never with an
# assumption that writing the file worked.
hdr "Perf profiling capability (issue #3249)"
PERF_CAP_LIB="$REPO_ROOT/scripts/perf-capability.sh"
# INITIALISE BEFORE THE GUARDS, never after (issue #3249 review). The gate below is
# read as `${PERF_SECTION_OK:-0}`, so an INHERITED `PERF_SECTION_OK=1` from the
# ambient environment would carry a macOS host — or a checkout with no
# scripts/perf-capability.sh — straight into the Linux-only implementation, calling
# helper functions that were never sourced. Every variable this section reads is
# initialised here, so no ambient export can steer any of it.
PERF_SECTION_OK=0
PERF_DROPIN=''
PERF_DROPIN_OK=0
PERF_TOKEN=''
PERF_TOKEN_BEFORE=''
if [ ! -r "$PERF_CAP_LIB" ]; then
  warn "scripts/perf-capability.sh missing from this checkout — perf capability UNVERIFIED"
elif [ "$PLATFORM" != linux ]; then
  info "perf_event_paranoid/kptr_restrict are Linux kernel controls — nothing to configure on $PLATFORM"
else
  # shellcheck source=scripts/perf-capability.sh
  . "$PERF_CAP_LIB"
  # FAIL CLOSED on a misused test seam BEFORE anything privileged happens: this
  # section pipes the drop-in through `sudo tee <path>`, so a stray
  # CQLITE_PERF_SYSCTL_DIR / CQLITE_PERF_PROC_DIR export must never steer that write
  # or fabricate a /proc verdict. The seams are inert without CQLITE_PERF_TEST_MODE=1;
  # under the marker each must be provably INSIDE the declared sandbox root, and the
  # marker itself forbids a reachable real sudo/sysctl (see scripts/perf-capability.sh).
  if ! perf_capability_env_guard; then
    warn "perf capability SKIPPED — the test-only env seams are misconfigured (a seam set without CQLITE_PERF_TEST_MODE=1; or test mode without a proven CQLITE_PERF_TEST_SANDBOX and both path seams strictly inside it, which has no production fallback; or test mode with a reachable real sudo/sysctl — details on stderr) — refusing to run a privileged write against an env-chosen path"
    PERF_SECTION_OK=0
  else
    PERF_SECTION_OK=1
  fi
fi
if [ "$PERF_SECTION_OK" = 1 ]; then
  # FAIL CLOSED if the write target cannot even be NAMED (issue #3261 AC1): the resolver refuses
  # an out-of-sandbox seam AND a managed name that is itself a symlink. An empty PERF_DROPIN would
  # otherwise flow into the messages and the write as an empty path, so the section stops here.
  if ! PERF_DROPIN=$(perf_capability_dropin_path) || [ -z "$PERF_DROPIN" ]; then
    warn "perf capability SKIPPED — the drop-in write target could not be resolved (an out-of-sandbox test seam, or the managed name is a SYMLINK a privileged write would follow — details on stderr); wrote nothing"
    PERF_SECTION_OK=0
  fi
fi
if [ "$PERF_SECTION_OK" = 1 ]; then

  # perf_apply_now: run (under --yes) or PRINT (check mode) `sysctl -q --system`,
  # recording THREE INDEPENDENT FACTS instead of collapsing them into one rc:
  #   PERF_APPLY_RAN  1 iff the command actually executed (0 = check mode printed it)
  #   PERF_APPLY_RC   that command's exit status when it ran ('' otherwise)
  # and, separately, the /proc read-back the caller performs afterwards.
  #
  # WHY THE SPLIT (issue #3249 review). `sysctl --system` applies EVERY drop-in on the
  # box, so it can apply OURS perfectly and still exit non-zero because an unrelated
  # pre-existing entry failed (a stale /etc/sysctl.conf line, a foreign drop-in naming
  # a knob this kernel lacks). Gating the read-back on its rc therefore left the token
  # STALE and printed "nothing was applied" on a box that had just become profileable —
  # a FALSE verdict, and the verdict is the whole point of AC2. "The apply command
  # failed" and "the controls are not in effect" are independent facts and are reported
  # as such: the read-back happens after EVERY attempt, whatever the rc.
  PERF_APPLY_RAN=0
  PERF_APPLY_RC=''
  perf_apply_now() {
    PERF_APPLY_RAN=0
    PERF_APPLY_RC=''
    local -a cmd=(${PERF_ROOT[@]+"${PERF_ROOT[@]}"} sysctl -q --system)
    if [ "$AUTO_YES" != 1 ]; then
      info "apply the drop-in now:  ${cmd[*]}"
      return 0
    fi
    info "apply the drop-in now: ${cmd[*]}"
    PERF_APPLY_RAN=1
    "${cmd[@]}"
    PERF_APPLY_RC=$?
    return 0
  }

  # Privilege is OPTIONAL: probe it non-interactively (`sudo -n`) so bootstrap can
  # never block on a password prompt on an unattended worker.
  # Every privileged call below goes through PERF_ROOT (an ARRAY: empty when we are
  # already root) and always carries `-n`, so no code path can ever sit on a
  # password prompt.
  #
  # `sudo -n` failing is TWO different boxes and they need DIFFERENT remedies: no
  # sudo binary at all (a printed `sudo tee` line is useless there — the fix is a
  # root shell or the image owner) versus a sudo that needs a password (the same line
  # works, just interactively). PERF_PRIV_STATE distinguishes them.
  #
  # The root test goes through perf_capability_self_uid_into, which reports rc 1 for an
  # UNKNOWN identity instead of substituting a plausible uid (issue #3249 review R4-1):
  # `$(id -u || echo 1000)` invented an unprivileged answer whenever `id` was missing or
  # broken. Here an unknown identity simply is not root, so the `sudo -n` probe below
  # decides — the fail-closed direction (no privilege claimed, nothing written unless
  # sudo actually works).
  PERF_ROOT=()
  PERF_PRIV=0
  PERF_PRIV_STATE=unknown
  PERF_SELF_UID=''
  if perf_capability_self_uid_into PERF_SELF_UID && [ "$PERF_SELF_UID" = 0 ]; then
    PERF_PRIV=1; PERF_PRIV_STATE=root
  elif ! have sudo; then
    PERF_PRIV_STATE=no-sudo-binary
  elif bounded 10 sudo -n true >/dev/null 2>&1; then
    PERF_PRIV=1; PERF_ROOT=(sudo -n); PERF_PRIV_STATE=sudo-nopasswd
  else
    PERF_PRIV_STATE=sudo-needs-password
  fi
  # Prefix for PRINTED remedy lines: empty when we are already root (a printed
  # `sudo` would be wrong there, and on many root images sudo is not even
  # installed), plain `sudo` (never `-n`) otherwise, since a human running the line
  # by hand may legitimately be prompted.
  PERF_RUN_AS=""
  [ "$PERF_PRIV_STATE" = root ] || PERF_RUN_AS="sudo "

  # perf_remedy_line: the COMPLETE write-AND-APPLY remedy. Write-only was the bug —
  # an operator who pasted it had a reboot-persistent file and an unprofileable box
  # until the next reboot, which is precisely what the functional verification exists
  # to prevent, on the path most people take (no --yes).
  perf_remedy_line() {
    # THE PRINTED REMEDY POINTS AT BOOTSTRAP ITSELF (issue #3261, roborev rounds 5-7).
    # History, because it is the whole reason this is three lines instead of a clever one:
    #   * originally `--drop-in | sudo tee <path>` (and `--drop-in > <path>` from a root shell) — both
    #     open the destination BY NAME, so a symlink planted between this advice printing and the human
    #     running it redirects a privileged write. Hardening the installer while printing that is worse
    #     than not hardening it: it reads as safe.
    #   * then a new `perf-capability.sh --install` entry point — which itself shipped an env-guard
    #     bypass (round 6) and then a supplied-prefix bypass (round 7), i.e. a fresh public surface that
    #     re-opened the hole AC4 had just closed, twice.
    #   * now: no new surface at all. `bootstrap --yes` ALREADY performs the guarded staged install and
    #     applies `sysctl --system`, on the path this suite has always asserted. Removing the escape
    #     hatch is strictly safer than guarding it, and subtraction cannot introduce a false pass.
    # Bootstrap is idempotent (file header), so re-running it is the sanctioned repair everywhere.
    # THREE states, THREE remedies — a remedy that cannot work on the box it is printed for is
    # worse than none, because the user spends a cycle before learning that (roborev round 8, Medium,
    # a regression THIS branch introduced when it pointed every case at `--yes`).
    if [ "$PERF_PRIV_STATE" = no-sudo-binary ]; then
      info "no 'sudo' on this box — write + apply from a ROOT shell:  bash scripts/bootstrap-agent-machine.sh --yes"
      info "(or ask the image/host owner to install it; without the drop-in this box reverts to perf_event_paranoid=4 on reboot)"
    elif [ "$PERF_PRIV_STATE" = sudo-needs-password ]; then
      # Bootstrap probes privilege NON-INTERACTIVELY (`sudo -n`, see the probe above), so telling this
      # box to "re-run with --yes" would fail again in exactly the same way and never prompt. The user
      # must supply the password FIRST — `sudo -v` refreshes the credential timestamp, after which the
      # `sudo -n` inside bootstrap succeeds — or run from an already-authenticated root shell.
      info "sudo needs a password here, and bootstrap probes with 'sudo -n' (never prompts) — authenticate first, then re-run:  sudo -v && bash scripts/bootstrap-agent-machine.sh --yes"
      # NOT `sudo -i` (roborev round 11, Low): a login shell switches to root HOME, so the relative
      # `scripts/...` path below it usually would not exist — advice that fails on the box it is
      # printed for. `sudo bash <script>` prompts once, keeps the working directory, and makes
      # bootstrap itself root, so its internal `sudo -n` probe is never reached.
      info "(or simply:  sudo bash scripts/bootstrap-agent-machine.sh --yes)"
    else
      info "write + apply the drop-in:  bash scripts/bootstrap-agent-machine.sh --yes"
    fi
  }

  # perf_apply_remedy_line: the APPLY-ONLY remedy, for the box whose drop-in is ALREADY
  # current — re-writing the file is not the fix there, applying it is. Same no-sudo /
  # needs-a-password / already-root split as perf_remedy_line above, because a printed
  # `sudo` is un-runnable on a box with no sudo binary and wrong on a root box.
  perf_apply_remedy_line() {
    if [ "$PERF_PRIV_STATE" = no-sudo-binary ]; then
      info "no 'sudo' on this box — apply it from a ROOT shell:  sysctl -q --system"
      info "(or ask the image/host owner; the drop-in is already on disk, so this is the only step left)"
    else
      info "apply the drop-in now:  ${PERF_RUN_AS}sysctl -q --system"
    fi
  }

  # perf_diagnose_token <token>: translate a non-ok token into the ACTIONABLE
  # sentence. Shared by both non-ok paths below, so the diagnosis can never be
  # reachable from one and silently missing from the other.
  perf_diagnose_token() {
    case "$1" in
      paranoid-*) info "perf_event_paranoid >= 1 forbids CPU-WIDE events, so 'perf stat -C <cpu>' is DENIED — this is a PERMISSION verdict, not a missing capability" ;;
      kptr-restricted) info "kptr_restrict != 0 — kernel frames resolve to bare addresses (a SILENT attribution loss, not an error)" ;;
      absent) info "/proc/sys/kernel/{perf_event_paranoid,kptr_restrict} not present — a container without a writable procfs cannot be tuned from here; tune the HOST" ;;
      unknown) info "the /proc controls are present but unparseable — never guessed; inspect them by hand" ;;
    esac
    case "$1" in paranoid-*|kptr-restricted) perf_name_competitors ;; esac
  }

  # perf_name_competitors: NAME THE FILE THAT IS FIGHTING US, rather than reporting only
  # that the value did not take. Stock Ubuntu ships
  # /etc/sysctl.d/10-kernel-hardening.conf with `kernel.kptr_restrict = 1`, and that is
  # the concrete mechanism behind the "it silently reverts" note in three separate
  # measurement reports (ws0-3217, ws3-3029, the 2026-07-27 Cassandra baseline) — none of
  # which identified a cause. A named file is actionable; "it reverts" is not.
  #
  # A competitor that sorts AFTER our 99- drop-in is an ACTUAL override (sysctl.d is
  # applied in basename order, last assignment wins) and is reported as such; one that
  # sorts before is reported as harmless, which also documents WHY the 99- prefix is
  # load-bearing and must never be "tidied" to a lower number.
  #
  # THE SCAN COVERS THE WHOLE `sysctl --system` SEARCH PATH (issue #3249 review R5-4) —
  # /etc/sysctl.d, /run/sysctl.d, /usr/local/lib/sysctl.d, /usr/lib/sysctl.d,
  # /lib/sysctl.d and /etc/sysctl.conf — with same-basename masking honoured, because a
  # later-sorting file in /run or /usr/lib overriding us while this reported "no
  # competitor" is the same silent-revert mystery wearing a different directory.
  perf_name_competitors() {
    local scan verdict path found=0
    # A FAILED scan is reported as a failed scan, never as "no competitors" — the whole
    # point of this diagnostic is to replace an unknown with a named file, so silently
    # printing the reassuring line on an unreadable directory would recreate the mystery.
    if ! scan=$(perf_capability_competing_files); then
      info "could not scan the 'sysctl --system' search path for competing perf_event_paranoid/kptr_restrict settings — inspect it by hand"
      return 0
    fi
    while IFS=' ' read -r verdict path; do
      [ -n "$path" ] || continue
      found=1
      case "$verdict" in
        override) warn "OVERRIDE: $path also sets perf_event_paranoid/kptr_restrict and its name sorts AFTER $PERF_CAPABILITY_DROPIN_BASENAME, so it is applied LAST and WINS — fix or rename that file" ;;
        last)     warn "OVERRIDE: $path also sets perf_event_paranoid/kptr_restrict and is applied AFTER every sysctl.d drop-in (both by 'sysctl --system' and systemd-sysctl), so it WINS regardless of our filename — fix that file" ;;
        *)        info "competing file: $path also sets perf_event_paranoid/kptr_restrict but sorts BEFORE $PERF_CAPABILITY_DROPIN_BASENAME, so ours wins (this is exactly why the '99-' prefix is load-bearing — never rename the drop-in)" ;;
      esac
    done <<EOF
$scan
EOF
    [ "$found" = 1 ] || info "no other file on the 'sysctl --system' search path (/etc/sysctl.d, /run/sysctl.d, /usr/local/lib/sysctl.d, /usr/lib/sysctl.d, /lib/sysctl.d, /etc/sysctl.conf) sets perf_event_paranoid/kptr_restrict"
  }

  # perf_inspect_lines: where a value that did not take actually comes from.
  # /etc/sysctl.conf is applied AFTER every sysctl.d drop-in by both
  # `sysctl --system` and systemd-sysctl, so a stale entry there BEATS our 99- file —
  # listing only /etc/sysctl.d would hide the most likely culprit.
  perf_inspect_lines() {
    info "inspect:  sysctl -a --pattern 'perf_event_paranoid|kptr_restrict'; grep -Hn 'perf_event_paranoid\|kptr_restrict' /etc/sysctl.conf /etc/sysctl.d/*.conf /run/sysctl.d/*.conf /usr/local/lib/sysctl.d/*.conf /usr/lib/sysctl.d/*.conf /lib/sysctl.d/*.conf"
    info "precedence: /etc/sysctl.conf is applied AFTER the sysctl.d drop-ins (both by 'sysctl --system' and systemd-sysctl), so a stale entry THERE overrides our 99- file"
  }

  PERF_TOKEN_BEFORE=$(perf_capability_token)
  info "runtime now: perf_event_paranoid=$(perf_capability_proc_value perf_event_paranoid || echo '<unreadable>') kptr_restrict=$(perf_capability_proc_value kptr_restrict || echo '<unreadable>')  (gate stamps perf=$PERF_TOKEN_BEFORE)"

  # ---- 1. the reboot-surviving drop-in (AC1), applied idempotently ----
  if perf_capability_dropin_current; then
    ok "drop-in already current: $PERF_DROPIN (survives reboot) — wrote nothing"
    PERF_DROPIN_OK=1
  elif [ "$PERF_PRIV" = 0 ]; then
    PERF_DROPIN_OK=0
    if [ "$PERF_PRIV_STATE" = no-sudo-binary ]; then
      warn "no 'sudo' binary on this box — cannot install $PERF_DROPIN; it reverts to perf_event_paranoid=4 on reboot"
    else
      warn "sudo needs a password (sudo -n failed) and bootstrap never prompts — cannot install $PERF_DROPIN unattended; it reverts to perf_event_paranoid=4 on reboot"
    fi
    perf_remedy_line
  else
    PERF_DROPIN_OK=0
    if [ "$AUTO_YES" = 1 ]; then
      info "writing perf sysctl drop-in: $PERF_DROPIN"
      # ATOMIC DIRECTORY-ENTRY REPLACEMENT, not `tee <path>` (issue #3261 AC1). `tee` opens
      # O_WRONLY|O_CREAT|O_TRUNC and FOLLOWS a symlink, so a symlink at the managed name aimed
      # this privileged write at the link's target — anywhere on the box. The helper writes a
      # staging entry in the validated directory and renames it over the name, so a pre-existing
      # symlink is REPLACED rather than written through, and it re-reads the file to confirm.
      perf_capability_dropin_install ${PERF_ROOT[@]+"${PERF_ROOT[@]}"}; PERF_INS_RC=$?
      if [ "$PERF_INS_RC" -eq 0 ]; then
        ok "wrote $PERF_DROPIN (kernel.perf_event_paranoid = -1, kernel.kptr_restrict = 0)"
        PERF_DROPIN_OK=1
      elif [ "$PERF_INS_RC" -eq 2 ]; then
        # UNSUPPORTED HOST, not a failed attempt: re-running cannot help, so the remedy line is
        # deliberately NOT printed — advice that cannot work is worse than none (roborev round 16).
        warn "cannot install $PERF_DROPIN on this host: the atomic staged install needs GNU coreutils"
      else
        warn "could NOT write $PERF_DROPIN"
        perf_remedy_line
      fi
    else
      perf_remedy_line
      info "(re-run with --yes to write AND apply it automatically)"
    fi
  fi

  # ---- 2. apply now + READ BACK from /proc (never trust the write's rc) ----
  # A `sysctl -w`/`--system` can report success while the value does not take
  # (container, read-only /proc, a later-sorting drop-in overriding ours), so the
  # verdict is always the value read back out of /proc/sys/kernel.
  PERF_TOKEN=$(perf_capability_token)
  if [ "$PERF_TOKEN" = ok ]; then
    ok "kernel controls verified from /proc: perf_event_paranoid=$(perf_capability_proc_value perf_event_paranoid) kptr_restrict=$(perf_capability_proc_value kptr_restrict)"
  elif [ "$PERF_DROPIN_OK" = 1 ] && [ "$PERF_PRIV" = 1 ]; then
    perf_apply_now
    # READ BACK after EVERY attempt, regardless of the command's exit status: the rc
    # says something about `sysctl --system`, the /proc read says something about THIS
    # BOX, and only the latter is the capability verdict.
    if [ "$PERF_APPLY_RAN" = 1 ]; then
      PERF_TOKEN=$(perf_capability_token)
      # FACT 1, reported on its own line and never mixed with the verdict: the command
      # itself failed. It applies every drop-in on the box, so this may be an unrelated
      # entry — it does NOT mean our controls are unset (the read-back below answers
      # that, and the two lines are allowed to disagree).
      if [ "${PERF_APPLY_RC:-0}" != 0 ]; then
        warn "'sysctl -q --system' exited $PERF_APPLY_RC — it applies EVERY sysctl drop-in on this box, so an UNRELATED pre-existing entry may be the failure; the perf verdict below comes from /proc, not from this exit code"
        perf_inspect_lines
      fi
    fi
    # FACT 2, independent of FACT 1: what /proc reports now.
    if [ "$PERF_TOKEN" = ok ]; then
      ok "kernel controls READ BACK from /proc as profileable: perf_event_paranoid=$(perf_capability_proc_value perf_event_paranoid) kptr_restrict=$(perf_capability_proc_value kptr_restrict)"
    elif [ "$PERF_APPLY_RAN" = 1 ] && [ "${PERF_APPLY_RC:-0}" = 0 ]; then
      # The apply RAN and reported success, and /proc still disagrees — the silent
      # revert this whole section exists to catch.
      warn "sysctl --system reported success but /proc still reports perf=$PERF_TOKEN — the value did NOT take (container, read-only /proc, or a later-sorting drop-in / /etc/sysctl.conf overrides ours)"
      perf_diagnose_token "$PERF_TOKEN"
      perf_inspect_lines
    elif [ "$PERF_APPLY_RAN" = 1 ]; then
      # The command failed AND the controls are not in effect. Both facts are already
      # on the record; state only the second one here, with no claim about which
      # drop-in failed.
      warn "/proc still reports perf=$PERF_TOKEN after the apply — the controls are NOT in effect (and the apply command itself exited $PERF_APPLY_RC; see above)"
      perf_diagnose_token "$PERF_TOKEN"
    else
      # The apply did NOT run: check mode printed the line. Never claim it was applied.
      # Without this branch a check-mode run with a current drop-in and a non-ok
      # runtime printed NO warn at all and never counted a WARNING.
      warn "kernel controls NOT in the profileable state (gate stamps perf=$PERF_TOKEN) — the drop-in is on disk but has NOT been applied to the running kernel"
      info "apply it now (or re-run with --yes):  ${PERF_RUN_AS}sysctl -q --system"
      perf_diagnose_token "$PERF_TOKEN"
    fi
  else
    warn "kernel controls NOT in the profileable state (gate stamps perf=$PERF_TOKEN)"
    perf_diagnose_token "$PERF_TOKEN"
    # The drop-in is already on disk (so there is nothing to WRITE) but we have no
    # non-interactive privilege to apply it. Without this the branch diagnosed the
    # state and offered NO remedy at all, contradicting every other unprivileged path
    # in this section — the no-sudo / needs-a-password boxes are exactly the ones that
    # need a runnable command printed.
    if [ "$PERF_DROPIN_OK" = 1 ]; then
      info "the drop-in is on disk but has NOT been applied to the running kernel, and bootstrap has no non-interactive privilege to apply it"
      perf_apply_remedy_line
    fi
  fi

  # ---- 3. FUNCTIONAL verification (AC2), attributed to the RIGHT IDENTITY -------
  # Two rules, and together they make a FALSE "VERIFIED" unreachable (issue #3249
  # review):
  #
  # (a) OVERALL verification requires BOTH facts: the /proc token = ok AND a
  #     functional pass. Reporting the functional result alone as the verdict let a
  #     box whose /proc says `paranoid-2`/`kptr-restricted` print its diagnosis AND
  #     "VERIFIED" in the same run — contradictory output in which the reassuring line
  #     wins the reader's attention. Anything short of both facts is reported as
  #     PARTIAL DIAGNOSTIC INFORMATION, explicitly subordinate to the /proc verdict.
  #
  # (b) THE PRIVILEGE DIMENSION. perf_event_paranoid restricts UNPRIVILEGED users and
  #     ROOT BYPASSES IT. `sudo bash scripts/bootstrap-agent-machine.sh` is a normal
  #     invocation — arguably the most likely one, since this section needs root to
  #     write /etc/sysctl.d — and as root `perf stat -C 0 -e cycles` SUCCEEDS on a
  #     paranoid=4 box where every unprivileged agent process still gets EACCES. So a
  #     root-run probe cannot demonstrate the property we care about ("an UNPRIVILEGED
  #     process can collect CPU-wide cycles"). CHOSEN FIX: DROP PRIVILEGE for the
  #     probe when a mechanism exists (setpriv/runuser/`sudo -u`, targeting SUDO_UID
  #     — the account that invoked sudo — else `nobody`), because that measures the
  #     real property; and when no mechanism exists, label the root result as NOT
  #     evidence of unprivileged capability and let the identity-independent /proc
  #     token be the authority. Never imply the stronger claim.
  PERF_FUNC=untested            # untested | pass | fail
  PERF_VERIFY_OUT=''
  PERF_DROP_PREFIX=''
  PERF_DROP_STATE=''
  PERF_UNPRIV_EVIDENCE=0        # 1 iff the probe measured an UNPRIVILEGED process
  if ! have perf; then
    warn "perf MISSING — profiling capability UNVERIFIED on this machine"
    if have apt-get; then
      info "install perf:  ${PERF_RUN_AS}apt-get install -y linux-tools-common linux-tools-\$(uname -r)"
    else
      info "install perf via your distro's linux-tools/perf package"
    fi
  else
    if perf_capability_drop_prefix_into PERF_DROP_PREFIX PERF_DROP_STATE; then
      PERF_UNPRIV_EVIDENCE=1
    fi
    if [ -n "$PERF_DROP_PREFIX" ]; then
      info "this run is ROOT and root BYPASSES perf_event_paranoid, so the probe DROPS PRIVILEGE ($PERF_DROP_STATE) — otherwise it would measure root's capability, not an agent's"
    fi
    # shellcheck disable=SC2086  # deliberate split of our own literal prefix tokens
    if PERF_VERIFY_OUT=$(perf_capability_verify $PERF_DROP_PREFIX); then
      PERF_FUNC=pass
    else
      PERF_FUNC=fail
    fi
  fi
  if [ "$PERF_FUNC" = pass ] && [ "$PERF_TOKEN" = ok ] && [ "$PERF_UNPRIV_EVIDENCE" = 1 ]; then
    ok "perf capability VERIFIED — /proc reports perf=ok and an UNPRIVILEGED perf stat -C 0 -e cycles reports $PERF_VERIFY_OUT"
  elif [ "$PERF_FUNC" = pass ]; then
    # A functional PASS that is NOT a verification: either /proc disagrees, or the
    # probe could not be attributed to an unprivileged identity. Reported as partial
    # information and never as a verdict — no run may print a non-ok token diagnosis
    # and an unqualified "VERIFIED" together.
    warn "perf capability NOT verified — the 'perf stat -C 0 -e cycles' probe succeeded ($PERF_VERIFY_OUT), but that is PARTIAL DIAGNOSTIC INFORMATION only, subordinate to the /proc verdict (gate stamps perf=$PERF_TOKEN)"
    if [ "$PERF_TOKEN" != ok ]; then
      info "/proc is the AUTHORITY here: perf=$PERF_TOKEN, so an unprivileged agent process is still restricted whatever this probe reported"
      perf_diagnose_token "$PERF_TOKEN"
    fi
    if [ "$PERF_UNPRIV_EVIDENCE" = 0 ]; then
      # `identity-unknown` is its own sentence: with `id -u` unusable we do NOT know the
      # probe ran as root, and asserting it would be as unfounded as asserting it did
      # not (issue #3249 review R4-1). Report the unknown as an unknown.
      if [ "$PERF_DROP_STATE" = identity-unknown ]; then
        info "the identity of this process could NOT be determined ('id -u' unusable), so the probe's success is NOT attributable to an unprivileged process — no capability claim is made from it"
        info "install/repair coreutils so 'id -u' works, then re-run; or prove it as the agent account:  sudo -u <agent-user> perf stat -C 0 -e cycles -- sleep 0.1"
      else
        info "the probe ran AS ROOT ($PERF_DROP_STATE) and root BYPASSES perf_event_paranoid — its success is NOT evidence that an UNPRIVILEGED process can profile this box"
        info "prove it as the agent account:  sudo -u <agent-user> perf stat -C 0 -e cycles -- sleep 0.1   (or install util-linux so bootstrap can drop privilege itself via setpriv)"
      fi
    fi
  elif [ "$PERF_FUNC" = fail ]; then
    warn "perf capability NOT verified — perf stat -C 0 -e cycles: $PERF_VERIFY_OUT"
    info "an rc-0 perf stat with a zero/<not supported> counter is NOT a working setup — a virtualised or masked PMU counts nothing"
    if [ -n "$PERF_DROP_PREFIX" ]; then
      info "the probe ran with privilege DROPPED ($PERF_DROP_STATE), so this failure may be the drop mechanism rather than perf itself — reproduce exactly:  $PERF_DROP_PREFIX perf stat -C 0 -e cycles -- sleep 0.1"
    else
      info "reproduce by hand:  perf stat -C 0 -e cycles -- sleep 0.1"
    fi
  fi
  info "note: BPF collectors (bpftrace/bcc) still require sudo — a permissive perf_event_paranoid does NOT grant BPF map creation (#3217)"
  info "posture: this loosening is for DEDICATED SINGLE-TENANT measurement/agent boxes; never apply it to a shared or multi-tenant host"
fi

# ---- 3. GitHub board access + project scope (Path A, #1886; #2942) ----
# The board is the SOLE dispatch authority, so "can this machine use the board?"
# must be answered by TRYING it. Until #2942 this section matched the `project`
# scope STRING and declared "board dispatch works" — and a box was observed where
# that scope IS present, `gh project item-edit` fails for a missing `read:org`, and
# the equivalent `updateProjectV2ItemFieldValue` GraphQL mutation succeeds with the
# SAME token. A scope match is evidence about a token, not about the operation.
# The scope check survives as a cheap PRE-FILTER; the verdict comes from a probe.
# The probe is strictly READ-ONLY — a bootstrap must never mutate a real board item.
#
# Both facts the verdict rests on must also be attributed to the RIGHT IDENTITY.
# `gh auth status` prints one stanza PER logged-in account on a host and the active
# one is not guaranteed first, so a plain grep can read a different account's scopes
# than the one every gh call will use — this repo has a documented instance
# (.claude/skills/flow-board/SKILL.md: the active account silently flips to an EMU
# account lacking `project`, and board writes then degrade SILENTLY). And `flow-board`
# FORCES CQLITE_PROJECT_ACCOUNT active before any board op, so probing as whatever
# account happens to be active measures a different identity than board dispatch uses.
# Both are the same defect this section exists to remove, one level up: a verdict
# derived from something other than the operation actually performed.
BOARD_OWNER="${CQLITE_PROJECT_OWNER:-pmcfadin}"
BOARD_ACCOUNT="${CQLITE_PROJECT_ACCOUNT:-pmcfadin}"   # same var flow-board honors
BOARD_TITLE="${PROJECT_TITLE:-CQLite Delivery}"       # same title setup-project-board.sh uses
BOARD_GRAPHQL_WRITE="updateProjectV2ItemFieldValue"
# CQLITE_PROJECT_NUMBER must NOT be defaulted to a guess. flow-board reads
# `project_number="${CQLITE_PROJECT_NUMBER:-}"` and sets have_project=0 → "board
# unreachable, STOP" when it is unset, so defaulting to 1 here would print a green
# "board reachable" on a box where every flow-* skill refuses to dispatch — the same
# false green this section exists to kill, one layer out. Unset is a REPORTABLE STATE.
BOARD_NUMBER="${CQLITE_PROJECT_NUMBER:-}"
BOARD_NUMBER_SRC=env
[ -z "$BOARD_NUMBER" ] && BOARD_NUMBER_SRC=unset

# board_discover_number <owner> <title> — resolve the board number by TITLE, the way
# test-data/scripts/setup-project-board.sh does, so an unset env var can still be
# diagnosed precisely ("it is #N, you just haven't exported it") instead of vaguely.
# Needs jq (as that script does); prints nothing when unavailable or not found.
board_discover_number() {
  have jq || return 0
  bounded 20 gh project list --owner "$1" --format json --limit 200 2>/dev/null \
    | jq -r --arg t "$2" '(.projects // [])[] | select(.title == $t) | .number' 2>/dev/null \
    | head -n1
}

# gh_active_block <auth-status-output> — ONLY the active account's stanza. Stanzas
# start at a "Logged in to <host> account <name>" line; the active one carries
# "Active account: true". Falls back to the whole output when no such marker exists
# (older gh / single account), so this never returns empty.
gh_active_block() {
  printf '%s\n' "$1" | awk '
    /Logged in to/ { n++ }
    { blk[n] = blk[n] $0 "\n"; if ($0 ~ /Active account: true/) active = n }
    END {
      if (active) { printf "%s", blk[active]; exit }
      for (i = 0; i <= n; i++) printf "%s", blk[i]
    }'
}

# gh_block_account <stanza> — the account name a stanza belongs to ("" if unknown).
gh_block_account() {
  printf '%s\n' "$1" | sed -n 's/.*Logged in to [^ ]* account \([^ ][^ ]*\).*/\1/p' | head -1
}

# board_graphql_read <owner> <number> — READ-ONLY projectV2 lookup, the read
# counterpart of the write fallback, so its result says whether that fallback is
# actually available on this token. Success requires a NON-EMPTY project id:
# `gh api graphql` exits 0 on a well-formed query that resolves to null (e.g. an
# org-owned board queried as a user), which would otherwise read as a false OK.
# Tries user-owned first (what project-board-sync.yml uses), then org-owned.
board_graphql_read() {
  local owner="$1" number="$2" id
  [ -n "$number" ] || return 1
  id=$(bounded 20 gh api graphql -f owner="$owner" -F number="$number" \
        -f query='query($owner:String!,$number:Int!){user(login:$owner){projectV2(number:$number){id}}}' \
        --jq '.data.user.projectV2.id' 2>/dev/null)
  if [ -z "$id" ] || [ "$id" = null ]; then
    id=$(bounded 20 gh api graphql -f owner="$owner" -F number="$number" \
          -f query='query($owner:String!,$number:Int!){organization(login:$owner){projectV2(number:$number){id}}}' \
          --jq '.data.organization.projectV2.id' 2>/dev/null)
  fi
  [ -n "$id" ] && [ "$id" != null ]
}

# restore_board_account — put the operator's active gh account back. Idempotent, and
# installed as a TRAP: between the switch and the restore sit network calls, so a hang
# plus Ctrl-C, or a supervisor SIGTERM, would otherwise kill the script mid-bracket and
# SILENTLY leave the active account changed. A check must never mutate host state.
restore_board_account() {
  [ "${BOARD_SWITCHED:-0}" = 1 ] || return 0
  BOARD_SWITCHED=0
  if gh auth switch --user "$PRE_PROBE_ACCOUNT" >/dev/null 2>&1; then
    info "restored gh's active account to '$PRE_PROBE_ACCOUNT'"
  else
    warn "could NOT restore gh's active account — it is left as '$BOARD_ACCOUNT'. Fix: gh auth switch --user $PRE_PROBE_ACCOUNT"
  fi
}

hdr "GitHub board access + project scope (Path A, #1886)"
if have gh; then
  # ONE invocation: capture output and status together (gh auth status hits the API).
  auth_out=$(gh auth status 2>&1); auth_rc=$?
  if [ "$auth_rc" -eq 0 ]; then
    ok "gh authenticated"
    # ---- attribute every fact below to the ACTIVE account, and say which one ----
    ACTIVE_BLOCK=$(gh_active_block "$auth_out")
    ACTIVE_ACCOUNT=$(gh_block_account "$ACTIVE_BLOCK")
    # An env token outranks the keyring, so there is exactly ONE identity and
    # `gh auth switch` cannot change it — detect that up front rather than
    # attempting (and reporting) a switch that could never take effect.
    GH_ENV_TOKEN=0
    [ -n "${GH_TOKEN:-${GITHUB_TOKEN:-}}" ] && GH_ENV_TOKEN=1
    info "measuring gh account '${ACTIVE_ACCOUNT:-<unknown>}'$([ "$GH_ENV_TOKEN" = 1 ] && printf ' (from GH_TOKEN in the environment)')"

    # Mirror flow-board: it forces CQLITE_PROJECT_ACCOUNT active before EVERY board
    # op, so probing as a different account would answer a question nobody asks —
    # loudly failing a box where dispatch works, or greenlighting an account dispatch
    # never uses. Switching mutates real gh state, so we only switch when we must and
    # we always switch BACK; a failed restore is a loud warn, never a silent leftover.
    BOARD_SWITCHED=0
    PRE_PROBE_ACCOUNT="$ACTIVE_ACCOUNT"   # what the operator had active; restored below
    if [ "$GH_ENV_TOKEN" = 0 ] && [ -n "$ACTIVE_ACCOUNT" ] && [ "$ACTIVE_ACCOUNT" != "$BOARD_ACCOUNT" ]; then
      if gh auth switch --user "$BOARD_ACCOUNT" >/dev/null 2>&1; then
        BOARD_SWITCHED=1
        # Arm the restore BEFORE anything that can hang or be interrupted.
        trap 'restore_board_account' EXIT
        trap 'restore_board_account; exit 130' INT
        trap 'restore_board_account; exit 143' TERM
        info "temporarily switched gh's active account '$ACTIVE_ACCOUNT' -> '$BOARD_ACCOUNT' for the probe (what flow-board does); will switch back"
        auth_out=$(gh auth status 2>&1)
        ACTIVE_BLOCK=$(gh_active_block "$auth_out")
        ACTIVE_ACCOUNT=$(gh_block_account "$ACTIVE_BLOCK")
      else
        info "could not switch to '$BOARD_ACCOUNT' (CQLITE_PROJECT_ACCOUNT) — probing as '$ACTIVE_ACCOUNT'; flow-board would attempt the same switch"
      fi
    fi

    # Token-boundary match on the ACTIVE account's scopes line so a scope like
    # 'project:read-only' (or any 'xprojecty' substring) can never false-positive.
    scopes_line=$(printf '%s\n' "$ACTIVE_BLOCK" | grep "Token scopes:" | head -1)
    scope_prefilter=0
    if printf '%s\n' "$scopes_line" | grep -qE "(^|[ ,'])project([ ,']|$)"; then
      scope_prefilter=1
      info "pre-filter: 'project' scope present on '${ACTIVE_ACCOUNT:-<unknown>}' (NOT the verdict — the probe below decides)"
    else
      warn "'project' scope MISSING on gh account '${ACTIVE_ACCOUNT:-<unknown>}' — the board is the SOLE dispatch authority (Path A). Fix:"
      info "gh auth refresh -s project"
    fi
    # gh's OWN declaration that the ACTIVE token lacks scopes gh requires. This is
    # the discriminator behind the observed false OK: scopes include 'project',
    # `read:org` is missing, and `gh project item-edit` fails on that alone.
    missing_scopes=$(printf '%s\n' "$ACTIVE_BLOCK" | grep -i "Missing required token scopes" | head -1)
    missing_list="${missing_scopes##*scopes:}"; missing_list="${missing_list# }"
    # Resolve the board number when it is not exported, so the gap can be reported
    # precisely rather than papered over with a guess.
    if [ "$BOARD_NUMBER_SRC" = unset ]; then
      BOARD_NUMBER="$(board_discover_number "$BOARD_OWNER" "$BOARD_TITLE")"
      [ -n "$BOARD_NUMBER" ] && BOARD_NUMBER_SRC=discovered
    fi

    # READ-ONLY probes, both BOUNDED: they sit between the account switch and its
    # restore, so an unbounded hang here is what strands the operator's account.
    board_read=0
    graphql_read=0
    if [ -n "$BOARD_NUMBER" ]; then
      bounded 20 gh project view "$BOARD_NUMBER" --owner "$BOARD_OWNER" >/dev/null 2>&1 && board_read=1
      board_graphql_read "$BOARD_OWNER" "$BOARD_NUMBER" && graphql_read=1
    fi

    # Restore the operator's account BEFORE reporting (the trap is the backstop for an
    # interrupt; this is the normal path, and it is idempotent with the trap).
    restore_board_account

    # An unqualified ok requires ALL THREE: the read probe passed, gh reports no
    # missing required scopes, AND the 'project' WRITE scope is present. Without the
    # last one a read-only token (e.g. read:project) would print the earlier
    # "'project' scope MISSING" warn and then a reassuring ok as the section's LAST
    # word, while every dispatch write still fails.
    # An unexported CQLITE_PROJECT_NUMBER is a DISPATCH BLOCKER even when every probe
    # passes, because flow-board defaults it to empty and stops. Report it first: it is
    # the one gap the operator can fix with a single export.
    if [ "$BOARD_NUMBER_SRC" != env ]; then
      if [ "$BOARD_NUMBER_SRC" = discovered ]; then
        warn "CQLITE_PROJECT_NUMBER is NOT exported — flow-* skills default it to EMPTY and STOP ('board unreachable'), whatever this probe says"
        info "the board titled '$BOARD_TITLE' is #$BOARD_NUMBER — export it:  export CQLITE_PROJECT_NUMBER=$BOARD_NUMBER"
      else
        warn "CQLITE_PROJECT_NUMBER is NOT exported and no board titled '$BOARD_TITLE' could be resolved for owner '$BOARD_OWNER' — flow-* skills will STOP (Path A: no board, no dispatch)"
        info "run:  bash test-data/scripts/setup-project-board.sh    (it discovers/creates the board and prints the export line)"
      fi
    fi

    if [ "$board_read" = 1 ] && [ -z "$missing_scopes" ] && [ "$scope_prefilter" = 1 ] \
       && [ "$BOARD_NUMBER_SRC" = env ]; then
      ok "board #$BOARD_NUMBER ($BOARD_OWNER) reachable as '${ACTIVE_ACCOUNT:-<unknown>}' — 'gh project' read probe OK, 'project' scope present, no missing token scopes"
    elif [ "$board_read" = 1 ]; then
      board_dq=""
      [ "$scope_prefilter" = 0 ] && board_dq="the 'project' WRITE scope is MISSING"
      if [ -n "$missing_scopes" ]; then
        [ -n "$board_dq" ] && board_dq="$board_dq; "
        board_dq="${board_dq}gh reports missing required scopes ($missing_list)"
      fi
      if [ "$BOARD_NUMBER_SRC" != env ]; then
        [ -n "$board_dq" ] && board_dq="$board_dq; "
        board_dq="${board_dq}CQLITE_PROJECT_NUMBER is not exported"
      fi
      warn "board READ works as '${ACTIVE_ACCOUNT:-<unknown>}' but $board_dq — board dispatch can still FAIL"
      info "board WRITES: fall back to the GraphQL \`$BOARD_GRAPHQL_WRITE\` mutation (it succeeds with the SAME token; graphql projectV2 read probe: $([ "$graphql_read" = 1 ] && echo OK || echo FAILED))"
      info "or widen the token:  gh auth refresh -s project -s read:org"
    elif [ "$graphql_read" = 1 ]; then
      warn "'gh project' CANNOT use board #$BOARD_NUMBER ($BOARD_OWNER) as '${ACTIVE_ACCOUNT:-<unknown>}'$([ "$scope_prefilter" = 1 ] && printf ' even though the scope pre-filter passed')${missing_list:+ — gh reports missing required scopes ($missing_list)}"
      info "the GraphQL projectV2 read probe SUCCEEDED with the same token — board WRITES must go through the \`$BOARD_GRAPHQL_WRITE\` mutation, not \`gh project item-edit\`"
      info "or widen the token:  gh auth refresh -s read:org"
    else
      warn "board #${BOARD_NUMBER:-<unresolved>} ($BOARD_OWNER) UNREACHABLE as '${ACTIVE_ACCOUNT:-<unknown>}' — BOTH probes failed ('gh project view' and the GraphQL projectV2 read). Path A: a session with no board access must STOP, never label-dispatch"
      info "check the account (CQLITE_PROJECT_ACCOUNT=$BOARD_ACCOUNT) and owner/number (CQLITE_PROJECT_OWNER / CQLITE_PROJECT_NUMBER), then: gh auth refresh -s project -s read:org"
      info "neither 'gh project item-edit' nor the \`$BOARD_GRAPHQL_WRITE\` GraphQL fallback can work until a probe passes"
    fi
  else
    warn "gh not authenticated — run: gh auth login (then gh auth refresh -s project)"
  fi
else
  warn "gh CLI NOT installed — $(brew_or_cargo gh gh 2>/dev/null || echo 'install GitHub CLI: https://cli.github.com')"
fi

# ---- 3b. git push credentials (issue #2942) ----
# `gh` auth and `git` auth are SEPARATE credential paths. An authenticated gh CLI is
# NOT evidence that a raw `git push` can authenticate — and the flow tooling
# (scripts/flow/claim.sh, scripts/flow/claim-heartbeat.sh) pushes with plain git on
# 10+ call sites (the claim ref, the adoption CAS, release, heartbeats). On a box
# where only gh is wired, every one of those fails with
#     fatal: could not read Username for 'https://github.com'
# so the claim protocol — the cross-machine lock the whole fleet depends on — simply
# does not work, while `gh auth status` reports a happy machine.
#
# The probe is `git credential fill`: it runs the CONFIGURED helper chain and answers
# exactly the question that matters ("would git find a credential for this host?")
# without contacting the network and without pushing anything. The filled credential
# is held only in a local shell variable — never printed, logged, or stored.
#
# The helper DECLINES (exit 1) when no token is in the environment rather than
# emitting an empty password. That matters because git treats a `password=` line as
# satisfied even when the value is EMPTY, so an empty-emitting helper produces a
# green probe on a machine where every push fails. This is highly reachable: --yes
# writes the helper globally and PERSISTENTLY while $GH_TOKEN is a per-shell env var,
# so bootstrapping in an interactive shell and then running the supervisor from
# systemd/cron would otherwise yield a green bootstrap and a dead claim protocol —
# the very "validated the configuration, not the operation" defect this change exists
# to kill. The probe correspondingly requires a NON-EMPTY password, not exit 0.
GIT_CRED_HELPER='!f(){ test "$1" = get || exit 0; t="${GH_TOKEN:-${GITHUB_TOKEN:-}}"; [ -n "$t" ] || exit 1; echo username=x-access-token; echo "password=$t"; };f'

# git_cred_probe <host> — 0 iff the configured helper chain yields a non-empty
# secret for <host>. Three hang guards, because this runs inside the gate's
# tooling-tests against a developer's REAL helper chain:
#   - GIT_TERMINAL_PROMPT=0 + a deliberately nonexistent askpass stop GIT's own prompt;
#   - `timeout` bounds a HELPER SUBPROCESS, which neither variable governs — a Git
#     Credential Manager device-code/browser flow, a credential-cache waiting on a
#     dead daemon socket, or a locked osxkeychain would otherwise block indefinitely.
# The output is captured into a variable rather than piped into grep so that grep -q
# closing the pipe early can never turn a SIGPIPE into a false "no credential".
git_cred_probe() {
  local host="$1" out
  out=$(printf 'protocol=https\nhost=%s\n\n' "$host" \
    | bounded 10 env GIT_TERMINAL_PROMPT=0 GIT_ASKPASS=cqlite-bootstrap-no-askpass \
      SSH_ASKPASS=cqlite-bootstrap-no-askpass \
      git -C "$REPO_ROOT" credential fill 2>/dev/null) || true
  printf '%s\n' "$out" | grep -q '^password=.'
}

# Both helper-inspection predicates below use a --get-regexp over the whole
# `credential.*.helper` key space rather than the bare `credential.helper` key. The
# helper THIS script writes is host-scoped (credential.https://<host>.helper), as is
# anything `gh auth setup-git` writes — so a bare-key lookup would miss exactly the
# configurations the script itself creates, and the two advisories that matter most to
# an unattended worker would go silent on the very machines they were written for.

# git_global_helper_configured — 0 iff ANY global credential helper exists, scoped or not.
git_global_helper_configured() {
  git config --global --get-regexp '^credential\..*helper$' >/dev/null 2>&1
}

# git_local_helper_configured — 0 iff ANY repo-local credential helper exists.
git_local_helper_configured() {
  git -C "$REPO_ROOT" config --local --get-regexp '^credential\..*helper$' >/dev/null 2>&1
}

# gh_token_is_authoritative_for_host <host> — 0 iff the ENVIRONMENT token this script
# would install ($GH_TOKEN, else $GITHUB_TOKEN) is the very token `gh` holds FOR THAT
# EXACT HOST. Gates the fallback repair below, which configures git to hand that token to
# whatever host the helper is scoped to (issue #3369 review).
#
# The host comes from LOCAL GIT CONFIG (`git remote get-url --push`), so a typo, a
# leftover fork/mirror pushurl or a stale `insteadOf` names a host nobody intended — and
# .agent-ami/profile.yaml runs this script with --fix-credentials at every onboard, so the
# accident path is automatic. An invoker who controls the box is out of the threat model;
# a MISCONFIGURED REMOTE is not, and by this repo's triage rule ("can be bypassed BY
# ACCIDENT ⇒ defect") that makes it in scope.
#
# THE PREDICATE IS TOKEN AUTHORITY, NOT A LOGIN. The first cut asked
# `gh auth status --hostname <host>`, which answers "does gh hold SOME credential for that
# host?" — a different fact from "is THIS token the credential for that host?". On a box
# authenticated to both github.com and a GitHub Enterprise host, with `origin` on the
# enterprise host, that check PASSES and the repair then hands the github.com token to the
# enterprise host: a proxy standing in for the property, the same shape as every other
# defect in this change. So `gh` is asked for the host's OWN token and it must MATCH.
#
# `gh` is the AUTHORITY and is asked directly. Nothing here allowlists hosts, and nothing
# infers enterprise-ness (or anything else) from the hostname string. The branch is keyed
# on the AFFIRMATIVE answer — an absent gh, a bounded-call failure, no token for that
# host, an empty answer and a genuine mismatch all land on the same NON-confirming side,
# because an unmeasured authority is not an authority. Neither value is ever printed,
# logged or compared through a file: both stay in local shell variables.
gh_token_is_authoritative_for_host() {
  local host="$1" env_tok gh_tok
  [ -n "$host" ] || return 1
  have gh || return 1
  env_tok="${GH_TOKEN:-${GITHUB_TOKEN:-}}"
  [ -n "$env_tok" ] || return 1
  gh_tok=$(bounded 20 gh auth token --hostname "$host" 2>/dev/null) || return 1
  [ -n "$gh_tok" ] || return 1
  [ "$gh_tok" = "$env_tok" ]
}

# git_env_token_helper_active — 0 iff a configured helper (any scope, any host key) is
# an ENV-DEREFERENCING one. Used to attach the "$GH_TOKEN must be exported" caveat to
# an otherwise-green verdict. Both markers must appear in the SAME line (--get-regexp
# prints one key+value per line): a helper with a token BAKED IN also says
# x-access-token but has no environment dependency, and claiming otherwise would be
# its own small misattribution.
git_env_token_helper_active() {
  { git config --global --get-regexp '^credential\..*helper$' 2>/dev/null
    git -C "$REPO_ROOT" config --get-regexp '^credential\..*helper$' 2>/dev/null
  } | grep -F 'x-access-token' | grep -qF 'GH_TOKEN'
}

# git_origin_host <url> — host of an http(s) origin ("" for any other form). The
# path is stripped FIRST so an '@' inside the path can never be mistaken for the
# user[:password]@ prefix.
#
# The userinfo is stripped at the LAST '@', not the first (issue #3369 review). That is
# where git itself splits an authority, and it matters here because this value is PRINTED:
# with `#*@`, a URL whose password contains an '@' (e.g. `https://u:p@ss@host/…`) left the
# tail of the password in the "host" and put a credential fragment into onboarding logs.
git_origin_host() {
  local url="$1" rest hostport
  case "$url" in
    https://*) rest="${url#https://}" ;;
    http://*)  rest="${url#http://}" ;;
    *) return 0 ;;
  esac
  hostport="${rest%%/*}"
  printf '%s' "${hostport##*@}"
}

# ---- the ONE remote this whole section is about (issue #3369 review) ----
# Resolved ONCE, and every consumer below — the credential probe, the repair, and the
# push probe — reads THIS value. The first cut derived credentials from `origin`'s FETCH
# url while the push probe pushed to `${CLAIM_REMOTE:-origin}`, so with CLAIM_REMOTE set
# (test_claim_lock.sh drives claim.sh exactly that way) or an `origin` carrying a
# `pushurl`, the credential half wired and blessed host A while the probe pushed to host
# B — a verdict about a host that is not the subject, and a second route to blocker 1's
# symptom (`--fix-credentials --strict` failing on a validly configured box).
#
# `get-url --push` is deliberately ONE call for both cases: it returns the `pushurl`
# when one is configured and falls back to the fetch url when none is. A second host
# variable is what created the defect, so there is exactly one.
PUSH_PROBE_REMOTE="${CLAIM_REMOTE:-origin}"   # the remote claim.sh itself will use

hdr "git push credentials (issue #2942)"
# Classify the remote BEFORE probing: only an http(s) remote uses a credential helper.
# An SSH remote authenticates with a key (a helper is irrelevant), and a local/file
# remote needs no credential at all — mislabeling either would send an operator
# after the wrong fix, which is the exact failure mode this whole change exists to end.
GIT_ORIGIN_URL=""; GIT_ORIGIN_HOST=""; GIT_ORIGIN_KIND=none
if have git; then
  GIT_ORIGIN_URL=$(git -C "$REPO_ROOT" remote get-url --push "$PUSH_PROBE_REMOTE" 2>/dev/null || true)
  case "$GIT_ORIGIN_URL" in
    "")                    GIT_ORIGIN_KIND=none ;;
    https://*|http://*)    GIT_ORIGIN_KIND=https; GIT_ORIGIN_HOST=$(git_origin_host "$GIT_ORIGIN_URL") ;;
    ssh://*|git+ssh://*)   GIT_ORIGIN_KIND=ssh ;;
    *@*:*)                 GIT_ORIGIN_KIND=ssh ;;   # scp-like git@host:owner/repo.git
    *)                     GIT_ORIGIN_KIND=other ;; # file:// or a local path
  esac
  [ "$GIT_ORIGIN_KIND" = https ] && [ -z "$GIT_ORIGIN_HOST" ] && GIT_ORIGIN_KIND=other
fi

if ! have git; then
  warn "git NOT installed — the claim protocol pushes with plain git"
elif [ "$GIT_ORIGIN_KIND" = none ]; then
  warn "no '$PUSH_PROBE_REMOTE' remote in $REPO_ROOT — cannot check push credentials"
elif [ "$GIT_ORIGIN_KIND" = ssh ]; then
  # SSH (git@host:… / ssh://…): git authenticates with your SSH key, and an https
  # credential helper is irrelevant. Report it and configure nothing.
  # NOT an exemption from the push probe below: an SSH origin is push-probed like any
  # other (the smoke push works over ssh), so a machine with no usable key still gets a
  # FAILED verdict rather than a green "no helper needed" (issue #3369).
  ok "'$PUSH_PROBE_REMOTE' is an SSH remote — git push authenticates via SSH keys, not a credential helper (no helper needed)"
  info "verify separately if pushes fail:  ssh -T git@github.com"
elif [ "$GIT_ORIGIN_KIND" = other ]; then
  info "'$PUSH_PROBE_REMOTE' is a '$GIT_ORIGIN_KIND' remote (neither http(s) nor SSH) — no credential helper applies"
elif git_cred_probe "$GIT_ORIGIN_HOST"; then
  # Says ONLY what `git credential fill` proved: a configured helper answered. It does
  # NOT say a push would succeed — the previous wording ("git push credentials resolve
  # for <host>") claimed push resolution on the strength of a configuration probe that
  # never contacts the network, which is the #3369 overclaim in one sentence. The push
  # claim belongs to the push probe below, and only to it.
  ok "a git credential helper ANSWERS for $GIT_ORIGIN_HOST with a non-empty secret (configuration only — push capability is measured in the next section)"
  if git_local_helper_configured && ! git_global_helper_configured; then
    info "note: the helper is configured at REPO-LOCAL scope only — a fresh clone or a"
    info "      new checkout on this box will NOT inherit it. Re-run with --yes to add a global one."
  fi
  if git_env_token_helper_active; then
    info "note: that helper reads \$GH_TOKEN from the ENVIRONMENT, so it works only in shells"
    info "      where GH_TOKEN is exported — a systemd/cron worker started without it will"
    info "      fail every push. For unattended workers prefer:  gh auth setup-git"
  fi
else
  # UNWIRED AS FOUND. Whether that is a WARNING depends on the machine's state when this
  # section ENDS, not on its state when the section began (issue #3369). The first cut
  # warned here, before the repair below, and nothing could retract it: with
  # --fix-credentials the run then REPAIRED the box, the push probe reported VERIFIED,
  # and the summary still withheld "All checks green." and --strict still exited 1 — so
  # the AMI onboarder's verify FAILED on exactly the box it had just fixed, which is the
  # whole scenario #3369 exists for. Every test stayed green while it did.
  #
  # So the diagnosis is gathered first, ONE verdict is emitted last, and it reports the
  # FINAL state. WARNINGS is never decremented: a counter that can go down would let an
  # unrelated later success cancel a genuine earlier fault. The fix is not to retract a
  # warning — it is not to emit one until the answer is known.
  cred_diag() {
    info "symptom: every push fails with  fatal: could not read Username for 'https://$GIT_ORIGIN_HOST'"
    info "impact: scripts/flow/claim.sh + claim-heartbeat.sh push on 10+ call sites — the claim protocol does not work"
    info "fix:    gh auth setup-git    (preferred; wires gh as git's credential helper)"
    info "        or re-run with --fix-credentials (or --yes) to configure a helper that reads \$GH_TOKEN at call time"
  }
  # --fix-credentials wires ONLY this section, leaving every other check read-only
  # (issue #3369): the AMI onboarder's verify step needs the credential path wired
  # after token injection, and turning a VERIFICATION step into a full toolchain
  # installer (which --yes also is) would be a far larger change than that needs.
  if [ "$AUTO_YES" = 1 ] || [ "$FIX_CREDENTIALS" = 1 ]; then
    info "git push has NO credentials for $GIT_ORIGIN_HOST yet — an authenticated 'gh' does NOT authenticate git; attempting to configure one"
    cred_fixed=0
    cred_how=""
    # Set only by the refusal branch below, so the ONE verdict can name what happened
    # without a second warning. It is a state of the FINAL machine (no credential was
    # configured, deliberately), which is what this section reports.
    cred_refused_host=0
    # Preferred form: let gh wire itself in. Verified by RE-PROBING — on the box
    # that motivated #2942 the gh credential path was precisely what was not wired,
    # so "the command exited 0" is not evidence. Every branch below is likewise
    # confirmed by a re-probe, never by the repair command's own exit status.
    if have gh && gh auth status >/dev/null 2>&1; then
      info "configuring: gh auth setup-git"
      if gh auth setup-git >/dev/null 2>&1 && git_cred_probe "$GIT_ORIGIN_HOST"; then
        cred_fixed=1; cred_how="'gh auth setup-git'"
      else
        info "'gh auth setup-git' did not yield a usable credential — falling back to the \$GH_TOKEN helper"
      fi
    fi
    if [ "$cred_fixed" = 0 ]; then
      if [ -z "${GH_TOKEN:-${GITHUB_TOKEN:-}}" ]; then
        # `info`, not `warn`: the single verdict below owns the warning, so a failed
        # repair counts ONCE rather than twice.
        info "cannot auto-configure: neither GH_TOKEN nor GITHUB_TOKEN is set in this environment"
        info "export GH_TOKEN=<token>, then re-run:  bash scripts/bootstrap-agent-machine.sh --fix-credentials"
      elif ! gh_token_is_authoritative_for_host "$GIT_ORIGIN_HOST"; then
        # AFFIRMATIVE AUTHORITY OR NO REPAIR (issue #3369 review). The fallback below
        # would configure git to hand the ENVIRONMENT token to $GIT_ORIGIN_HOST, and the
        # push probe in §3b-push then performs a real push to it — all from a host
        # resolved out of local git config, which a typo, a leftover fork/mirror pushurl
        # or a stale `insteadOf` can point anywhere. So the repair is gated on that token
        # being the one `gh` holds FOR THAT HOST, and refuses otherwise.
        # `gh auth setup-git` above stays unconditional: it makes gh the helper and gh
        # decides what it answers for, per host. The dangerous path is specifically
        # dereferencing a RAW token for a host that token does not belong to.
        cred_refused_host=1
        info "REFUSING to configure a \$GH_TOKEN-dereferencing helper for $GIT_ORIGIN_HOST: 'gh auth token --hostname $GIT_ORIGIN_HOST' did not return the token this environment holds (gh may hold none for that host, or a DIFFERENT one — e.g. a github.com token on a box that also has a GitHub Enterprise host)"
      else
        # The value written is a shell snippet that DEREFERENCES $GH_TOKEN when git
        # asks — the token itself never lands on disk, so rotating it needs no
        # reconfiguration and a leaked ~/.gitconfig leaks no credential. (Decision 2
        # of the worker-env-preflight change: explicitly NOT ~/.git-credentials.)
        #
        # HOST-SCOPED, never a bare [credential] helper. An unscoped helper offers the
        # GitHub token to EVERY https host git talks to — a submodule, a cargo/pip git
        # dependency, a mistyped clone, or any host that answers 401 would receive it.
        # `gh auth setup-git` (the preferred path this falls back FROM) scopes per host,
        # so an unscoped fallback would make --yes strictly less safe than its own
        # preferred branch. "A leaked ~/.gitconfig leaks no credential" is true of the
        # env-var indirection but says nothing about who git hands the token TO.
        cred_key="credential.https://$GIT_ORIGIN_HOST.helper"
        info "configuring $cred_key to dereference \$GH_TOKEN at call time (scoped to $GIT_ORIGIN_HOST; the token is NOT written to disk)"
        # Idempotence: never stack a second copy on a re-run. The check is the EXACT
        # key we write — a copy scoped to some other host is unrelated and must not
        # suppress this one.
        if git config --global --get-all "$cred_key" 2>/dev/null | grep -qF 'x-access-token'; then
          info "a \$GH_TOKEN-style helper is ALREADY configured for $GIT_ORIGIN_HOST yet the probe still fails — check that GH_TOKEN is set, valid and unexpired (not re-adding it)"
        elif git config --global --add "$cred_key" "$GIT_CRED_HELPER" 2>/dev/null \
           && git_cred_probe "$GIT_ORIGIN_HOST"; then
          cred_fixed=1; cred_how="a \$GH_TOKEN-dereferencing helper"
        else
          info "could not configure a working git credential helper"
        fi
      fi
    fi
    # THE SINGLE VERDICT, on the state the machine is in NOW.
    if [ "$cred_fixed" = 1 ]; then
      ok "git credentials WIRED BY THIS RUN via $cred_how for $GIT_ORIGIN_HOST (confirmed by re-probe; no secret written to disk)"
      if git_env_token_helper_active; then
        info "this helper reads \$GH_TOKEN from the ENVIRONMENT — an unattended worker (systemd/cron)"
        info "started without GH_TOKEN exported will still fail every push; prefer 'gh auth setup-git' there"
      fi
    elif [ "$cred_refused_host" = 1 ]; then
      # A DISTINCT verdict, not a second one: exactly one warning is still emitted, and it
      # reports the final state (no credential configured) together with WHY, because the
      # remedy differs completely from the generic "could not configure any" case below —
      # there the operator supplies a credential; here the operator first decides whether
      # that host should be receiving a GitHub token at all.
      #
      # The HOST is named; the push URL deliberately is NOT. A remote URL can embed a
      # credential (claim.sh classifies push stderr for exactly that reason and never
      # prints it), so echoing it here would trade one leak for another.
      warn "git push has NO credentials for $GIT_ORIGIN_HOST and this run REFUSED to configure any: \$GH_TOKEN is not the token 'gh' holds for that host, so a token will NOT be configured for it"
      info "why: the fallback helper hands that token — a real credential — to whichever host it is scoped to, and this host came from LOCAL GIT CONFIG ('git remote get-url --push $PUSH_PROBE_REMOTE'), which a typo, a leftover fork/mirror pushurl or a stale 'insteadOf' can point anywhere. A login for the host is NOT enough: on a box authenticated to both github.com and a GitHub Enterprise host, the token for one must not be handed to the other"
      info "fix (host is correct):   gh auth login --hostname $GIT_ORIGIN_HOST    then re-run with --fix-credentials (preferably WITHOUT \$GH_TOKEN set, so 'gh auth setup-git' supplies that host's own token)"
      info "fix (host is WRONG):     git -C $REPO_ROOT remote set-url --push $PUSH_PROBE_REMOTE <the intended url>"
      info "impact until then: scripts/flow/claim.sh + claim-heartbeat.sh push on 10+ call sites — the claim protocol does not work"
    else
      warn "git push has NO credentials for $GIT_ORIGIN_HOST and this run could NOT configure any — an authenticated 'gh' does NOT authenticate git"
      cred_diag
    fi
  else
    warn "git push has NO credentials for $GIT_ORIGIN_HOST — an authenticated 'gh' does NOT authenticate git"
    cred_diag
    info "(re-run with --fix-credentials to wire credentials only, or --yes to also install)"
  fi
fi
# ---- 3b-push. git PUSH CAPABILITY — measured, not inferred (issue #3369) ----
# Everything above this line validates CONFIGURATION. `git credential fill` runs the
# configured helper chain and answers "would git find a credential for this host?"
# without ever contacting the network and without pushing anything; the section's own
# comment says so. That is not the property the fleet depends on. A token with no
# `contents:write`, an expired or unrotated token, an SSO authorization never granted,
# and a host that refuses the `refs/claims/*` namespace ALL pass every check above and
# still fail the very first thing a lane does — `bash scripts/flow/claim.sh claim <N>`,
# a plain `git push` of a claim ref. That is the defect this exists for: the affected
# box passed `gh auth status` AND `git ls-remote origin HEAD` and therefore LOOKED
# healthy, so the preflight certified a machine on which no lane could start.
# Generalized in docs/development/agent-machine-setup.md: "A scope match is evidence
# about a token, not about the operation." Neither is a credential-helper answer, and
# neither is a read.
#
# So the verdict below comes from performing THE OPERATION: create + read back +
# delete a throwaway `refs/claims/smoke-<commit-sha>` ref. It DELEGATES to
# `scripts/flow/claim.sh smoke`, the repo's existing sanctioned push probe — which
# pushes the same ref namespace the claim protocol uses, already classifies an auth
# fault (`reason=auth`) apart from a namespace refusal, and always deletes the ref it
# created. `git push --dry-run` is deliberately NOT used: it stops short of the ref
# update, which is the part the server decides, so it is one more piece of evidence
# about configuration. It runs AFTER the auto-fix above, so what gets measured is the
# machine as the fix left it.
#
# THREE-VALUED, NEVER TWO. A positive verdict requires an AFFIRMATIVE measurement, so
# the `ok` branch is keyed on the smoke ref having really been created and read back —
# never on the absence of a failure signal. Anything unmeasurable (no remote, an
# unreachable remote, no claim.sh in this checkout, no `timeout`/`gtimeout` to bound
# the network calls, the bound firing) is UNMEASURED, which is a [warn]: an unmeasured
# push capability must not inherit the permissive branch, and "All checks green." must
# not be printed for a machine whose claim protocol was never exercised.
#
# Hang safety: the whole probe runs under `bounded`, with GIT_TERMINAL_PROMPT=0 and a
# deliberately nonexistent askpass, so neither a credential prompt nor a wedged remote
# can stall a boot.
#
# THE COST, STATED OUT LOUD, because it is paid on EVERY invocation of this script —
# a developer laptop run included, not just an image launch. Measuring the operation
# means performing it:
#   - two network round trips beyond the reachability read (the create push and the
#     cleanup delete), plus one `ls-remote`;
#   - a TRANSIENT `refs/claims/smoke-<commit-sha>` ref CREATED AND DELETED on the SHARED
#     origin. claim.sh's cmd_smoke describes itself as a "ONE-TIME preflight ... NOT
#     part of the hermetic test suite" because it mutates the real remote; invoking it
#     here makes that mutation routine, which is accepted deliberately: making the
#     measurement opt-in would restore "a read by default", the exact defect #3369
#     exists to remove.
# RESIDUAL, stated precisely because the two halves differ. An OBSERVED cleanup failure
# now FAILS the probe (`reason=cleanup-unverified`) instead of passing with a stderr
# warning, so it cannot pass silently. But a run KILLED between the create and the
# delete produces NO verdict at all and can still leave a `refs/claims/smoke-*` ref on
# the origin — nothing can close that window from inside the probe. So the cleanup
# commands stay documented:
#   git ls-remote origin 'refs/claims/smoke-*'
#   git push origin --delete refs/claims/smoke-<commit-sha>
# PUSH_PROBE_REMOTE is resolved ONCE above §3b — the credential half and this probe MUST
# address the same remote (see the note there).
CLAIM_SH="$REPO_ROOT/scripts/flow/claim.sh"
# How many destinations `git push $PUSH_PROBE_REMOTE` would write to. `--all` is what
# distinguishes "the first push URL" (what the classification above reads) from "every
# push URL" (what a push actually hits).
PUSH_PROBE_URL_COUNT=0
if have git; then
  PUSH_PROBE_URL_COUNT=$(git -C "$REPO_ROOT" remote get-url --push --all "$PUSH_PROBE_REMOTE" 2>/dev/null | grep -c . || true)
  PUSH_PROBE_URL_COUNT=${PUSH_PROBE_URL_COUNT:-0}
fi
PUSH_PROBE_BOUND=60   # 3 network round trips (push, ls-remote, delete) + slack

# git_push_probe_stderr_is_auth <text> — mirrors git_stderr_is_auth in
# scripts/flow/claim.sh (keep the two in sync). Used for ONE purpose: when the
# reachability precheck fails, tell a CREDENTIAL refusal (FAILED — actionable, names
# the fix) from an unreachable remote (UNMEASURED — nothing was learned about push).
# Deliberately conservative: anything unrecognized stays UNMEASURED rather than being
# asserted as a credential fault. The classified text is NEVER printed — a remote URL
# can carry an embedded token, which is why claim.sh never echoes git's stderr either.
git_push_probe_stderr_is_auth() {
  case "$1" in
    *"could not read Username"*      | *"could not read Password"*        | \
    *"Authentication failed"*        | *"authentication failed"*          | \
    *"terminal prompts disabled"*    | *"Invalid username or token"*      | \
    *"Invalid username or password"* | *"Permission denied (publickey)"*  | \
    *"Permission to "*" denied"*     | *"Write access to repository not granted"* | \
    *"Support for password authentication was removed"* | *"401 Unauthorized"*)
      return 0 ;;
  esac
  return 1
}

# push_probe_fix_advice — the remediation the AUTH verdicts print.
#
# PROTOCOL-AWARE, keyed on GIT_ORIGIN_KIND (#3369 review). That value is derived from the
# REMOTE URL in §3b — authoritative by construction — never from git's error text, so
# this adds no classification and no new failure cause. It matters because an SSH remote
# authenticates with a KEY: `gh auth setup-git` wires an https credential helper and
# cannot affect it, so advising it there sends the operator to fix something that is not
# even in the path. This change routed SSH origins into the push probe for the first
# time, which is what made that reachable.
#
# The scope line in the https branch STATES A POSSIBILITY rather than detecting one: a
# token can authenticate perfectly and still lack `contents:write`, which no credential
# rewiring fixes. We cannot tell that state from here and do not pretend to — the line
# points at the scope report the board-access section already prints.
push_probe_fix_advice() {
  info "impact: scripts/flow/claim.sh + claim-heartbeat.sh push on 10+ call sites — a lane cannot even START"
  # KEYED ON THE AFFIRMATIVE VALUE, never on `!= ssh` (#3369 review). The first cut said
  # "not SSH ⇒ give https credential advice", so `file://`, a bare local path and any
  # custom protocol were handed `gh auth setup-git` — guidance that cannot apply to them.
  # That is this change's own central rule broken by the change: a permissive branch must
  # test for the value it means, and GIT_ORIGIN_KIND already computes an explicit
  # `other`. Three affirmative arms, one per protocol class; nothing falls through into
  # advice written for a different transport.
  if [ "$GIT_ORIGIN_KIND" = ssh ]; then
    info "fix:    remote '$PUSH_PROBE_REMOTE' is an SSH remote — git authenticates with your SSH KEY, not a credential helper"
    info "        check the key:  ssh -T git@<host>   and that it is loaded:  ssh-add -l"
    info "        'gh auth setup-git' does NOT apply here — it configures https credentials only"
  elif [ "$GIT_ORIGIN_KIND" = https ]; then
    info "fix:    gh auth setup-git    (preferred; wires gh as git's credential helper)"
    info "        then re-run:  bash scripts/bootstrap-agent-machine.sh --fix-credentials"
    info "        if a helper is ALREADY wired, the token may authenticate yet lack WRITE access —"
    info "        check the scopes reported in the 'gh auth + board access' section above (contents:write / repo)"
  else
    info "fix:    remote '$PUSH_PROBE_REMOTE' is a '$GIT_ORIGIN_KIND' remote — neither https nor SSH, so a git"
    info "        credential helper may not apply at all; check that transport's own access path"
    info "        ('gh auth setup-git' configures https credentials only, and would not affect this remote)"
  fi
  # THE REMOTE URL IS NEVER PRINTED (issue #3369 review). Both non-https arms used to
  # print $GIT_ORIGIN_URL verbatim, and a remote URL can carry `https://user:token@host/…`
  # — while this script's output is persisted in onboarding logs, so those two lines wrote
  # a live credential into a log file. §3b already treats remote URLs as secret-bearing
  # (it classifies push stderr rather than echoing it); these sites had diverged from it.
  # What identifies the subject unambiguously is the remote NAME plus the protocol class,
  # both of which are printed above; the operator can read the URL locally.
  info "        (the URL is deliberately NOT printed here — it can embed a credential and this output is logged;"
  info "         read it locally with:  git -C $REPO_ROOT remote get-url --push $PUSH_PROBE_REMOTE)"
  info "verify by hand:  bash scripts/flow/claim.sh smoke"
}

hdr "git PUSH capability (issue #3369)"
if [ -n "$TIMEOUT_BIN" ] && [ "$TIMEOUT_KILL_AFTER" = 0 ]; then
  info "note: $TIMEOUT_BIN does not accept --kill-after, so every bound here degrades to SIGTERM-only —"
  info "      a child that ignores SIGTERM can still outlive its bound. Install GNU coreutils for the hard bound."
fi
if [ "$SKIP_PUSH_PROBE" = 1 ]; then
  # Loud and NON-PASSING by construction: an opt-out that returned `ok` would be a
  # switch for buying a vacuous green, which is the whole failure mode above.
  warn "git-push: OPT-OUT (--skip-push-probe) — push capability was NOT measured on this machine"
  info "this run cannot certify that the claim protocol works here; drop the flag to measure it"
elif ! have git; then
  warn "git-push: UNMEASURED (git is not installed — nothing to probe with)"
elif [ ! -f "$CLAIM_SH" ]; then
  warn "git-push: UNMEASURED (no scripts/flow/claim.sh under $REPO_ROOT — the sanctioned push probe is not in this checkout)"
elif ! git -C "$REPO_ROOT" remote get-url "$PUSH_PROBE_REMOTE" >/dev/null 2>&1; then
  warn "git-push: UNMEASURED (no '$PUSH_PROBE_REMOTE' remote in $REPO_ROOT — nothing to push to)"
elif [ "$PUSH_PROBE_URL_COUNT" -gt 1 ]; then
  # A remote may carry SEVERAL pushurls, and `git push <remote>` pushes to EVERY one, so
  # the probe would mutate N destinations while `get-url --push` describes only the first
  # (#3369 review). It could create the ref on A, fail on B, and return having cleaned
  # neither — an uninterpretable result from a MUTATING measurement. This change's own
  # discipline says do not run a measurement you cannot interpret, so it refuses instead
  # of enumerating destinations or growing per-destination credential logic. UNMEASURED,
  # so the machine is never certified on the strength of it.
  warn "git-push: UNMEASURED (remote '$PUSH_PROBE_REMOTE' has $PUSH_PROBE_URL_COUNT push URLs — refusing to run a MUTATING probe against multiple destinations)"
  info "verify by hand against one destination:  CLAIM_REMOTE=<single-url-remote> bash scripts/flow/claim.sh smoke"
elif [ "$TIMEOUT_KILL_AFTER" != 1 ] && [ -n "$TIMEOUT_BIN" ]; then
  # IF YOU CANNOT BOUND THE MUTATION, DO NOT MUTATE (#3369 review). `bounded` degrades to
  # SIGTERM-only when the resolved timeout lacks --kill-after, which keeps the NON-mutating
  # probes working — but this one performs a real network PUSH, and a SIGTERM-only bound
  # provably waits forever on a child that ignores SIGTERM (measured: 3s bound, 30s wait).
  # Hanging the launcher is worse than a red verdict, so the mutation is refused. Note the
  # fall-through to the probe below therefore requires the AFFIRMATIVE
  # TIMEOUT_KILL_AFTER=1 — a hard bound is a precondition of pushing, not a nice-to-have.
  warn "git-push: UNMEASURED (the resolved timeout cannot hard-kill (no --kill-after), so a wedged child could hang this run — refusing to perform a MUTATING push it cannot bound)"
  info "install GNU coreutils (macOS: brew install coreutils) so the probe can be hard-bounded, then re-run"
  info "or check by hand where a hang is survivable:  bash scripts/flow/claim.sh smoke"
elif [ -z "$TIMEOUT_BIN" ]; then
  # Refuse rather than run unbounded: an unbounded network push during boot can wedge
  # the onboarder indefinitely, and reporting `ok` for a probe we declined to run is
  # exactly the permissive-unknown branch this section rejects.
  warn "git-push: UNMEASURED (no timeout/gtimeout on PATH — refusing to run an UNBOUNDED network push during bootstrap)"
  info "install GNU coreutils so the probe can be bounded (macOS: brew install coreutils), then re-run"
else
  # Reachability precheck. Its ONLY job is to separate "the remote is unreachable, so
  # nothing was measured" from "the remote refused you" — it is NEVER a success
  # signal, because a passing read is precisely what made the broken box look healthy.
  push_probe_ls_err=""
  push_probe_ls_rc=0
  push_probe_ls_err=$(bounded "$PUSH_PROBE_BOUND" env GIT_TERMINAL_PROMPT=0 \
    GIT_ASKPASS=cqlite-bootstrap-no-askpass SSH_ASKPASS=cqlite-bootstrap-no-askpass \
    git -C "$REPO_ROOT" ls-remote "$PUSH_PROBE_REMOTE" 2>&1 >/dev/null) || push_probe_ls_rc=$?
  if [ "$push_probe_ls_rc" -ne 0 ] && git_push_probe_stderr_is_auth "$push_probe_ls_err"; then
    warn "git-push: FAILED (git cannot AUTHENTICATE to '$PUSH_PROBE_REMOTE' — an authenticated 'gh' does NOT authenticate git)"
    push_probe_fix_advice
  elif [ "$push_probe_ls_rc" -ne 0 ]; then
    warn "git-push: UNMEASURED (cannot reach '$PUSH_PROBE_REMOTE' — no network, or the remote does not exist; push capability is UNKNOWN, not ok)"
    info "re-run once the remote is reachable, or verify by hand:  bash scripts/flow/claim.sh smoke"
  else
    # THE OPERATION. Run from REPO_ROOT because claim.sh drives the git repo at $PWD.
    push_probe_out=""
    push_probe_rc=0
    push_probe_out=$(cd "$REPO_ROOT" && bounded "$PUSH_PROBE_BOUND" env GIT_TERMINAL_PROMPT=0 \
      GIT_ASKPASS=cqlite-bootstrap-no-askpass SSH_ASKPASS=cqlite-bootstrap-no-askpass \
      bash "$CLAIM_SH" smoke 2>&1) || push_probe_rc=$?
    # Every match below is ANCHORED on `^CLAIM: ` — claim.sh's `emit` prefix — and the
    # affirmative branch ALSO requires rc 0 (#3369). Unanchored, the verdict is decided
    # from a stream that carries claim.sh's own control tokens AND arbitrary payload
    # (remediation prose, an echoed command, a SMOKE-FAIL message that quotes another
    # verdict), so a data line could pose as the verdict — the control/data-in-one-channel
    # hazard CLAUDE.md documents. And a verdict token alone is a TEXT PROXY for a process
    # status: requiring both means a claim.sh that dies after printing cannot pass.
    if [ "$push_probe_rc" -eq 0 ] && printf '%s\n' "$push_probe_out" | grep -q '^CLAIM: SMOKE-OK'; then
      # The one affirmative branch: the ref was created on the remote, read back, AND
      # deleted — all three, because claim.sh now fails the delete rather than warning.
      ok "git-push: VERIFIED (refs/claims/* create+ls-remote+delete on '$PUSH_PROBE_REMOTE') — the claim protocol can run on this machine"
    elif printf '%s\n' "$push_probe_out" | grep -q '^CLAIM: SMOKE-FAIL.*reason=auth'; then
      warn "git-push: FAILED (git cannot AUTHENTICATE the refs/claims/* push to '$PUSH_PROBE_REMOTE' — an authenticated 'gh' does NOT authenticate git)"
      push_probe_fix_advice
    elif printf '%s\n' "$push_probe_out" | grep -q '^CLAIM: SMOKE-FAIL.*reason=commit-build'; then
      # A LOCAL failure building the throwaway claim commit: the push never happened,
      # so nothing was learned about push capability.
      warn "git-push: UNMEASURED (the throwaway claim commit could not be built locally — the push was never attempted)"
    elif printf '%s\n' "$push_probe_out" | grep -q '^CLAIM: SMOKE-FAIL'; then
      # THE CATCH-ALL QUOTES; IT DOES NOT RE-CLASSIFY (#3369 review). It used to re-word
      # every unrecognised reason code as "rejected the push — does the remote permit that
      # ref namespace?", which mis-attributed `ls-remote-mismatch` AND discarded the
      # cleanup detail claim.sh had just been fixed to report: a diagnostic improved in
      # one file, thrown away by its consumer one file over. Quoting claim.sh's own
      # verdict line means no reason code — present or FUTURE — can lose detail or be
      # given a cause bootstrap cannot know, and it is why the specific branches whose
      # only job was re-wording are gone. Dedicated branches survive ONLY where bootstrap
      # says something claim.sh cannot: `reason=auth` (the #2942 credential remediation)
      # and `reason=commit-build` (a LOCAL failure, so UNMEASURED rather than FAILED).
      # No credential advice here: the cause is unknown, and guessing it wrong is what
      # sent an operator after `gh auth setup-git` for a fault credentials cannot fix.
      warn "git-push: FAILED — the claim protocol cannot run on this machine until this is resolved. claim.sh reports:"
      printf '%s\n' "$push_probe_out" | grep '^CLAIM: SMOKE-FAIL' | while IFS= read -r push_probe_line; do
        info "$push_probe_line"
      done
    elif [ "$push_probe_rc" = 124 ] || [ "$push_probe_rc" = 137 ]; then
      # 124 = SIGTERM'd at the bound; 137 = it ignored SIGTERM and `bounded`'s
      # --kill-after escalated to SIGKILL. 137 was UNREACHABLE until that flag was added
      # (#3369 review): the code anticipated an outcome the wrapper could not produce.
      warn "git-push: UNMEASURED (the probe exceeded its ${PUSH_PROBE_BOUND}s bound and was killed — push capability is UNKNOWN, not ok)"
    else
      warn "git-push: UNMEASURED (scripts/flow/claim.sh smoke produced no SMOKE-OK/SMOKE-FAIL verdict, rc=$push_probe_rc — push capability is UNKNOWN, not ok)"
    fi
  fi
fi

# ---- 4. roborev (follows LOCAL machine config — never pin an agent, #1921 owner correction) ----
hdr "roborev (uses THIS machine's configured agent)"
if have roborev; then
  ok "roborev present"
  # Resolve THIS repo's configured agent from .roborev.toml (owner correction, #1921:
  # roborev follows the LOCAL machine's config — commonly codex — we NEVER prescribe
  # or pin a specific agent). Verify only that the *configured* agent resolves;
  # ignore unrelated agents that happen to be broken on this box.
  cfg_agent=""
  if [ -f "$REPO_ROOT/.roborev.toml" ]; then
    cfg_agent=$(grep -E "^agent[[:space:]]*=" "$REPO_ROOT/.roborev.toml" 2>/dev/null | head -1 | sed -E "s/.*=[[:space:]]*'?\"?([^'\"]*)'?\"?.*/\1/")
  fi
  info "configured agent (.roborev.toml) = ${cfg_agent:-<unset — roborev default>}"
  # `roborev check-agents --agent X` smoke-tests just that agent. Its exit code is 0
  # even when the agent is skipped (not on PATH), so parse the per-agent line: a
  # passing agent prints "... OK"; a missing/broken one prints "not found in PATH"
  # or "FAIL". Warn ONLY if the configured agent is broken — "fix or override per
  # your local setup", never prescribing a replacement.
  if [ -n "$cfg_agent" ]; then
    ra_out=$(roborev check-agents --agent "$cfg_agent" 2>&1)
    if printf '%s\n' "$ra_out" | grep -Eq "^[[:space:]]*[?[:alnum:]].*$cfg_agent.*\.\.\..*OK|^[0-9]+ passed" \
       && ! printf '%s\n' "$ra_out" | grep -Eq "not found in PATH|FAIL"; then
      ok "configured roborev agent '$cfg_agent' resolves (local config OK)"
    else
      warn "configured roborev agent '$cfg_agent' did not resolve — fix or override per your local setup"
      info "check: roborev check-agents --agent $cfg_agent   (config: .roborev.toml / roborev config list)"
    fi
  else
    if roborev check-agents >/dev/null 2>&1; then
      ok "roborev agents resolve (using roborev default; no repo override)"
    else
      warn "roborev's default agent did not resolve — fix or override per your local setup (roborev check-agents)"
    fi
  fi
else
  warn "roborev NOT installed — install per your setup, then it uses your local agent config (never a pinned agent)"
fi

# ---- 5. Datasets + CQLITE_DATASETS_ROOT guidance ----
hdr "Test datasets + CQLITE_DATASETS_ROOT"
# The datasets live in the MAIN checkout, not in worktrees (the Data.db binaries
# are gitignored and never copied into `git worktree add` trees). Resolve the main
# checkout via git's common dir so the guidance is correct even when bootstrap is
# run from a worktree.
MAIN_CHECKOUT="$REPO_ROOT"
common_dir=$(git -C "$REPO_ROOT" rev-parse --path-format=absolute --git-common-dir 2>/dev/null || true)
case "$common_dir" in
  */.git) MAIN_CHECKOUT=$(dirname "$common_dir") ;;
esac
MAIN_DATASETS="$MAIN_CHECKOUT/test-data/datasets"
DB_COUNT=$(find "$MAIN_DATASETS/sstables" -name '*-Data.db' 2>/dev/null | wc -l | tr -d ' ')
if [ "${DB_COUNT:-0}" -gt 0 ]; then
  ok "$DB_COUNT Data.db files under $MAIN_DATASETS/sstables"
else
  warn "no *-Data.db files found — integration/smoke tests need them"
  if [ "$AUTO_YES" = 1 ]; then
    info "fetching datasets: bash test-data/scripts/fetch-datasets.sh"
    if bash "$REPO_ROOT/test-data/scripts/fetch-datasets.sh"; then ok "datasets fetched"; else warn "dataset fetch FAILED"; fi
  else
    info "fetch:  bash test-data/scripts/fetch-datasets.sh   (re-run bootstrap with --yes to auto-fetch)"
  fi
fi
info "Guidance: point CQLITE_DATASETS_ROOT at the MAIN checkout's datasets."
info "  export CQLITE_DATASETS_ROOT=$MAIN_DATASETS"
info "  Worktrees lack the gitignored Data.db binaries — always aim CQLITE_DATASETS_ROOT"
info "  at the main checkout (above), NOT a worktree's own test-data/datasets."

# ---- 5b. Single-gate pin: one full gate per box (issues #2640 / #3414) ----
# The #1825 machine-wide cap admits N concurrent full gates, and the #2640 per-gate
# core budget derives each gate's core share from the SAME N — so pinning
# CQLITE_GATE_MAX_CONCURRENCY=1 admits exactly one full gate and hands it the FULL core
# count: no CPU oversubscription, no manual pgrep-serialization. A machine that
# deliberately wants >1 concurrent gate overrides the pin, and bootstrap NEVER rewrites
# an existing value.
#
# WHY THIS SECTION WAS REBUILT (issue #3414). It used to append the export to the shell
# PROFILE and then report `ok` from a GREP OF THAT PROFILE — or, worse, from the value
# it had INHERITED from its own caller. Both are PROXIES, and both were wrong on every
# box in the fleet at once: Ubuntu's stock ~/.bashrc opens with
# `case $- in *i*) ;; *) return;; esac`, so a line appended to it is never reached by
# the non-interactive shells that actually launch gates (a script, a `bash -c`, a
# detached `setsid`, a subagent). Measured: `ssh <box> 'echo $CQLITE_GATE_MAX_CONCURRENCY'`
# printed UNSET on all three boxes whose profiles held the export; every gate therefore
# resolved N=3 from the #1825 formula, and the live slot daemon's own argv said
# `--slots 3` while an isolation guarantee had been given to a measurement lane on the
# strength of the cap. PRESENCE IN A CONFIG FILE AND VISIBILITY TO THE PROCESS THAT
# READS IT ARE DIFFERENT FACTS, and only the second one matters.
#
# So this section does two separable things, and only the second one can produce a
# success verdict:
#   (1) PERSIST where a non-interactive shell provably reads it — /etc/environment,
#       read by PAM's pam_env at SESSION CREATION (`/etc/pam.d/sudo`, `/etc/pam.d/sshd`
#       and `/etc/pam.d/login` all carry `pam_env.so readenv=1`), with no interactivity
#       guard anywhere in the path. bash itself NEVER reads that file — measured,
#       `env -i bash -c` AND `env -i bash -lc` both report UNSET — which is exactly why
#       no shell-file check can answer this question, login shell or not.
#   (2) PROBE a fresh, profile-free session and report what it actually saw.
hdr "Single-gate pin (CQLITE_GATE_MAX_CONCURRENCY, issues #2640/#3414)"

# The PRODUCTION persistence target, written as a LITERAL here. The privileged write
# below can never name anything else (see the seam guard and the invariant assert).
PIN_ENV_FILE=/etc/environment
PIN_ENV_FILE_IS_SEAM=0
PIN_SECTION_OK=1
PIN_PERSIST_NOTE=""
# NO INLINE COMMENT ON THE VALUE LINE: pam_env parses `KEY=VALUE` literally, so a
# trailing `# ...` would become part of the value. Whole-line comments are skipped, so
# the rationale goes on its own line above it.
PIN_ENV_COMMENT='# cqlite: one full gate per box, full cores (issues #2640/#3414)'
PIN_ENV_VALUE='1'
PIN_ENV_LINE="CQLITE_GATE_MAX_CONCURRENCY=$PIN_ENV_VALUE"
EXPORT_LINE='export CQLITE_GATE_MAX_CONCURRENCY=1  # cqlite: one full gate per box, full cores (issue #2640)'

if [ "$SKIP_GATE_PIN" = 1 ]; then
  warn "gate-pin: OPT-OUT ($SKIP_GATE_PIN_HOW) — the pin was NOT persisted and its visibility was NOT measured"
  info "this run cannot certify that a non-interactive shell sees CQLITE_GATE_MAX_CONCURRENCY; drop the opt-out to measure it"
  PIN_SECTION_OK=0
fi

# PLATFORM SCOPING, EXPLICIT AND NOT INFERRED FROM FILE ABSENCE (#3414 roborev round 3).
# /etc/environment + pam_env is a Linux mechanism. Requiring the file for VERIFIED made a
# correctly-configured Mac PERMANENTLY non-passing under --strict — red on correct input,
# which is the shape this lane has now refused three times: an alarm that always fires is
# one people learn to waive, and it would have made `verify.run` unusable on any non-Linux
# host rather than merely unverified.
#
# Scoped on $PLATFORM, deliberately NOT on "is /etc/environment missing?", because those
# are different facts with opposite correct answers: a Mac has no such file BY DESIGN,
# while a LINUX box without one is a genuine anomaly that must stay non-passing. Folding
# them together would trade a false red for a false green.
#
# Reported as an explicit NON-FAILING verdict rather than silence: this repo's rule for
# the CI tier registry is that absence is an error while inapplicability must be reported
# as an explicit success, and the same reasoning applies to a provisioning check — a
# section that simply prints nothing is indistinguishable from one that did not run.
#
# macOS persistence (a launchd equivalent) is deliberately NOT implemented: there is no
# Mac on this fleet, so it could not be verified, and an unverifiable persistence path is
# worse than a documented gap. macOS is SCOPED OUT here, not supported.
# THE MECHANISM IS LINUX-SPECIFIC; THE REQUIREMENT IS NOT (#3414 final roborev). An
# earlier form emitted `ok "NOT-APPLICABLE"` here unconditionally, which let `--strict`
# CERTIFY AN UNPINNED non-Linux host: no /etc/environment exists to persist into, so the
# section declared inapplicability and passed — while every gate on that box resolved the
# #1825 cap from the formula. Inapplicability of the PERSISTENCE step was standing in for
# absence of the REQUIREMENT, which is this issue's own defect wearing a platform label.
#
# So the platform no longer earns an exemption; it earns a NARROWER QUESTION. We cannot
# ask "is it in the system env file" (there is none), but "does a fresh session see a
# value the GATE HONOURS" is platform-independent and is the fact that matters. That is
# decided below, in the same probe the Linux path uses, minus the file-correlation half.
PIN_PLATFORM_UNMANAGED=0
if [ "$PIN_SECTION_OK" = 1 ] && [ "$PLATFORM" != linux ]; then
  PIN_PLATFORM_UNMANAGED=1
  info "gate-pin: no PAM-read system-wide env file on this $PLATFORM host, so bootstrap does not MANAGE the pin here — but it still VERIFIES it: the probe below asks whether a fresh session sees a value the gate honours"
  info "to cap gates here, export CQLITE_GATE_MAX_CONCURRENCY in whatever this host's session-startup mechanism is"
fi

# Effective privilege, resolved BEFORE the seam guard because the guard now depends on it.
# Unknown is treated as privileged (fail closed): a seam that steers a write must not be
# admitted because we could not establish who we are.
# `$EUID` — Bash's own readonly — NEVER a PATH-resolved `id` (#3414 roborev round 8, HIGH).
# The privilege decision gates a seam that can steer a ROOT `tee -a` at an arbitrary
# absolute path, so a shadowed or merely MALFORMED `id` (a busybox variant, a broken PATH)
# makes a root invocation look unprivileged and reopens the round-5 High through a
# different door. A malformed `id` is an ACCIDENT, not an attack, which is what puts this
# inside the threat model rather than in the invoker-controls-the-process category.
# A nonnumeric value is treated as UNKNOWN, never as "not root": the guards below read an
# empty PIN_EUID as privileged, so an unreadable identity fails closed. Also one fewer fork.
PIN_EUID="${EUID-}"
case "$PIN_EUID" in ''|*[!0-9]*) PIN_EUID="" ;; esac

if [ "$PIN_SECTION_OK" = 1 ] && [ -n "${CQLITE_BOOTSTRAP_ENV_FILE:-}" ] \
   && { [ "$PIN_EUID" = 0 ] || [ -z "$PIN_EUID" ]; }; then
  # THE SEAM IS REFUSED OUTRIGHT UNDER ROOT (#3414 roborev round 5, HIGH).
  #
  # THIS IS THE SIXTH INSTANCE OF THIS ISSUE'S OWN DEFECT IN THIS LANE, AND IT WAS INSIDE
  # THE SAFETY GUARD ITSELF. The invariant below used to read `${#PIN_ROOT[@]} -gt 0`,
  # i.e. "are we going through sudo" — a PROXY for the fact that matters, which is
  # EFFECTIVE PRIVILEGE. When bootstrap is itself EUID 0, PIN_ROOT is empty and the
  # `tee -a` is privileged anyway, so "no env var can ever steer a PRIVILEGED write" was
  # FALSE under root: CQLITE_BOOTSTRAP_ENV_FILE could aim a root write at any absolute
  # path. Presence of a sudo prefix stood in for being privileged exactly as presence in
  # ~/.bashrc stood in for a gate seeing the pin.
  #
  # Refused rather than made safe: dropping to a validated UID would put MORE machinery
  # in the privileged path, and the seam exists precisely to avoid privileged writes. The
  # cost is real and accepted — the section cannot be exercised through the seam as root,
  # so a test that needs it must SKIP (counted), never report ok.
  warn "gate-pin: SKIPPED (CQLITE_BOOTSTRAP_ENV_FILE is set and this process is root or of unknown identity — refusing a seam that could steer a PRIVILEGED write at an env-chosen path)"
  info "the seam is for unprivileged self-tests only; run them as a normal user"
  PIN_SECTION_OK=0
fi

if [ "$PIN_SECTION_OK" = 1 ] && [ -n "${CQLITE_BOOTSTRAP_ENV_FILE:-}" ]; then
  # TEST-ONLY SEAM — fail-closed, with NO production fallback (the #3249 lesson: a test
  # seam that degrades to the real path certifies the real path by accident). It exists
  # so the self-tests can drive this section's DECISIONS without a privileged write to
  # the real /etc/environment, including when a suite happens to run as root. Two
  # properties keep it from being a hole of its own:
  #   * it is inert unless CQLITE_BOOTSTRAP_TEST_MODE=1, and a seam set WITHOUT the
  #     marker SKIPS the section rather than falling back, and
  #   * under the marker the write is UNPRIVILEGED (PIN_ROOT is forced empty below), so
  #     no environment variable can ever steer a PRIVILEGED write at a path of its
  #     choosing — the privileged branch only ever names the literal /etc/environment.
  if [ "${CQLITE_BOOTSTRAP_TEST_MODE:-0}" != 1 ]; then
    warn "gate-pin: SKIPPED (CQLITE_BOOTSTRAP_ENV_FILE is set without CQLITE_BOOTSTRAP_TEST_MODE=1) — refusing to persist the pin at an env-chosen path"
    PIN_SECTION_OK=0
  else
    case "$CQLITE_BOOTSTRAP_ENV_FILE" in
      /etc/environment)
        warn "gate-pin: SKIPPED (the test seam may not name the production /etc/environment)"
        PIN_SECTION_OK=0 ;;
      /*)
        if [ -L "$CQLITE_BOOTSTRAP_ENV_FILE" ]; then
          warn "gate-pin: SKIPPED (the test seam path is a SYMLINK — a write would follow it)"
          PIN_SECTION_OK=0
        elif [ ! -d "$(dirname "$CQLITE_BOOTSTRAP_ENV_FILE")" ]; then
          warn "gate-pin: SKIPPED (the test seam's parent directory does not exist)"
          PIN_SECTION_OK=0
        else
          PIN_ENV_FILE="$CQLITE_BOOTSTRAP_ENV_FILE"; PIN_ENV_FILE_IS_SEAM=1
        fi ;;
      *)
        warn "gate-pin: SKIPPED (CQLITE_BOOTSTRAP_ENV_FILE must be an ABSOLUTE path)"
        PIN_SECTION_OK=0 ;;
    esac
  fi
fi

if [ "$PIN_SECTION_OK" = 1 ]; then
  # Privilege, resolved the same way section 2c resolves it: NON-INTERACTIVELY, so no
  # code path here can ever sit on a password prompt on an unattended worker. The
  # runas target of the AVAILABILITY probe is the same as the runas target of the real
  # probe, so a box whose sudoers permits root-but-not-self cannot look available and
  # then fail in the measurement.
  PIN_ROOT=()
  PIN_PRIV_STATE=unknown
  # Initialised HERE, above the first thing that can set it: the non-root name check below
  # records a subject note, and an initialisation further down would blank it (the note
  # would vanish and the state would degrade to a bare no-identity).
  PIN_PROBE_SUBJECT_NOTE=""
  # AND THE NAME MUST MAP BACK TO $EUID (#3414 roborev round 9). `id -un` is a PATH lookup
  # against NSS, and both halves can lie: a shadowed or malformed `id`, or a stale NSS
  # mapping, can name an account that is NOT the one we are running as. The name is then
  # handed to `sudo -n -u`, so the probe would open a session for a DIFFERENT account and
  # report its PAM environment as ours — the wrong-subject defect the root branch below
  # already guards, on the path nobody thought to guard because it looked like it was just
  # asking who we are. $EUID is the authority (shell-set, no fork, unshadowable); the name
  # is a label for the messages and is only usable once it resolves back to that uid.
  # A mismatch is UNMEASURED, never a guess between two disagreeing sources.
  PIN_SELF_USER=$(id -un 2>/dev/null || true)
  if [ -n "$PIN_SELF_USER" ]; then
    pin_name_uid=$(id -u "$PIN_SELF_USER" 2>/dev/null || echo none)
    if [ "$pin_name_uid" != "$PIN_EUID" ]; then
      PIN_PROBE_SUBJECT_NOTE="'id -un' answered '$PIN_SELF_USER', which resolves to uid $pin_name_uid rather than this process's EUID $PIN_EUID — a shadowed 'id' or a stale NSS mapping, so probing that name would answer about the wrong account"
      PIN_SELF_USER=""
    fi
  fi
  PIN_SELF_UID="$PIN_EUID"   # one source: resolved before the seam guard, which needs it
  # UNDER `sudo bash bootstrap …` THE ANSWER TO `id -un` IS root, WHICH IS THE WRONG
  # SUBJECT (#3414 roborev round 5). Gates run as the agent account, and the two genuinely
  # diverge: a per-user `~/.pam_environment` on that account, or a sudoers rule that
  # supplies a value only for root's sessions, both make root's session say VERIFIED while
  # the account that matters gets something else. So when we are root and sudo told us who
  # invoked us, probe THAT account.
  #
  # The name is VALIDATED before use, and an unresolvable one is UNMEASURED rather than a
  # silent fall back to root: falling back would answer a question about the wrong user
  # and report it as if it were the right one, which is the substitution this whole
  # section exists to stop.
  # SUDO_UID IS THE AUTHORITY AND SUDO_USER MUST AGREE WITH IT (#3414 roborev round 8).
  # Trusting the NAME alone accepts stale or inconsistent metadata — `SUDO_UID=1000
  # SUDO_USER=root` would probe root and could report VERIFIED while the agent account has
  # a different PAM environment, which is the wrong-subject defect this retarget exists to
  # fix, wearing the retarget's own clothes. So: a validated NUMERIC SUDO_UID, NONZERO
  # (root invoking sudo tells us nothing new), and if a username is present it must resolve
  # to that same uid. Anything absent or inconsistent is UNMEASURED — never a fall back to
  # answering about root, and never a guess between two disagreeing sources.
  if [ "$PIN_EUID" = 0 ]; then
    pin_sudo_uid="${SUDO_UID-}"
    case "$pin_sudo_uid" in ''|*[!0-9]*) pin_sudo_uid="" ;; esac
    pin_sudo_name="${SUDO_USER-}"
    if [ -z "$pin_sudo_uid" ]; then
      PIN_SELF_USER=""
      PIN_PROBE_SUBJECT_NOTE="running as root with no usable SUDO_UID (absent or non-numeric), so the account a gate would run as is unknown"
    elif [ "$pin_sudo_uid" = 0 ]; then
      PIN_SELF_USER=""
      PIN_PROBE_SUBJECT_NOTE="SUDO_UID is 0, so sudo was invoked BY root — that tells us nothing about the account a gate runs as"
    else
      pin_resolved=$(id -un "$pin_sudo_uid" 2>/dev/null || true)
      if [ -z "$pin_resolved" ]; then
        PIN_SELF_USER=""
        PIN_PROBE_SUBJECT_NOTE="SUDO_UID $pin_sudo_uid does not resolve to an account"
      elif [ -n "$pin_sudo_name" ] && [ "$(id -u "$pin_sudo_name" 2>/dev/null || echo none)" != "$pin_sudo_uid" ]; then
        PIN_SELF_USER=""
        PIN_PROBE_SUBJECT_NOTE="INCONSISTENT sudo metadata — SUDO_USER '$pin_sudo_name' does not resolve to SUDO_UID $pin_sudo_uid, so which account to probe is ambiguous and neither answer would be trustworthy"
      else
        PIN_SELF_USER="$pin_resolved"
        PIN_PROBE_SUBJECT_NOTE="probed as '$pin_resolved' (uid $pin_sudo_uid, the account that invoked sudo), not root — root's session is not the one a gate runs in"
      fi
    fi
  fi
  if [ -z "$PIN_SELF_USER" ]; then
    PIN_PRIV_STATE=no-identity
    [ -n "$PIN_PROBE_SUBJECT_NOTE" ] && PIN_PRIV_STATE=invoker-unresolvable
  elif ! have sudo; then
    PIN_PRIV_STATE=no-sudo-binary
  elif [ -z "$TIMEOUT_BIN" ]; then
    # THE BOUND IS CHECKED BEFORE THE FIRST PROBE, NOT AFTER (#3414 roborev round 10).
    # `bounded` DEGRADES TO RUNNING THE COMMAND DIRECTLY when no timeout utility exists,
    # and both sudo probes used to execute ABOVE the later no-timeout guard — so on a host
    # with neither `timeout` nor `gtimeout` a stalled sudo/PAM/NSS lookup hung bootstrap
    # indefinitely WHILE THE CODE CLAIMED IT REFUSES UNBOUNDED SESSION PROBING. The false
    # claim is the worse half: a comment asserting a safety property the code does not
    # have is worse than not having it, because it stops the next person checking.
    PIN_PRIV_STATE=no-timeout-binary
  else
    # ROOT-WRITE PERMISSION AND SESSION-PROBE PERMISSION ARE INDEPENDENT FACTS, and gating
    # the probe on the WRITE one was a false red (#3414 roborev round 10). `sudo -n true`
    # asks "may I run ANYTHING as root"; the probe needs only "may I open a session as
    # MYSELF". A host granting the second but not the first — a narrowly-scoped sudoers
    # rule, or a box already correctly pinned — was reported sudo-needs-password and
    # failed --strict on a legitimately configured machine.
    #
    # This finishes a split started two rounds ago: PIN_WRITE_PRIV was separated from
    # PIN_PRIV_STATE in USE, but their ACQUISITION stayed entangled, so the weaker
    # permission still depended on the stronger one being granted first.
    if bounded 10 sudo -n -u "$PIN_SELF_USER" true >/dev/null 2>&1; then
      PIN_PRIV_STATE=sudo-nopasswd
    else
      PIN_PRIV_STATE=sudo-runas-denied
    fi
  fi
  # WRITE privilege: a SEPARATE question, asked separately and only where persistence is
  # attempted. A root shell with no sudo binary can persist but cannot probe; a box that
  # permits the runas-self session but not unrestricted root can probe but not persist.
  # Neither fact implies the other, so neither is derived from the other's answer.
  PIN_WRITE_PRIV=0
  if [ "$PIN_SELF_UID" = 0 ]; then
    PIN_WRITE_PRIV=1; PIN_ROOT=()
  elif have sudo && [ -n "$TIMEOUT_BIN" ] && bounded 10 sudo -n true >/dev/null 2>&1; then
    PIN_WRITE_PRIV=1; PIN_ROOT=(sudo -n)
  fi
  # Test mode never runs a PRIVILEGED write; the sandbox file belongs to the invoking
  # user. Forcing PIN_ROOT empty is what makes "no env var can steer a privileged
  # write" true IN CODE rather than merely by construction.
  if [ "$PIN_ENV_FILE_IS_SEAM" = 1 ]; then PIN_ROOT=(); PIN_WRITE_PRIV=1; fi

  # pin_strip_pam_quotes <raw>: reproduce pam_env's own de-quoting, so the value compared
  # below is the value a session actually RECEIVES.
  #
  # STRIPPING QUOTES IS READING THE FILE'S FORMAT; NORMALISING THE VALUE IS REINTERPRETING
  # IT. The first is mandatory, the second is forbidden. Without this, a legitimately
  # quoted line — `CQLITE_GATE_MAX_CONCURRENCY="1"`, and quoting IS the convention in this
  # file, which opens with `PATH="/usr/local/sbin:..."` — parses as `"1"`, the session
  # reports `1`, and a correctly-pinned box gets a non-passing verdict: red on correct
  # input, produced by the fix for a false green. But ` 1 ` must still mismatch `1`,
  # because the gate genuinely discards the former; that asymmetry is why the gate is
  # asked rather than second-guessed.
  #
  # THE RULE IS MEASURED, NOT ASSUMED (pam 1.5.3-5ubuntu5.7, this fleet's platform), by
  # writing shapes into the real /etc/environment and reading them back out of a fresh
  # session. It is NOT "a matched pair": a LEADING quote is stripped whether or not it is
  # closed, and single quotes behave like double ones — the symmetry this was told not to
  # assume, checked rather than guessed:
  #     "1"  -> 1        '1' -> 1        1   -> 1        "a b"  -> a b
  #     "1   -> 1        '1  -> 1        1"  -> 1"       a"b    -> a"b
  #     ""   -> (empty)  "   -> (empty)  " 1 " -> ` 1 `  ""1""  -> "1"
  # i.e. drop a leading `"` or `'`, then drop a trailing one of the SAME kind if present.
  #
  # WHY A WRONG ANSWER HERE IS SAFE IN THE DIRECTION THAT MATTERS: the session side of the
  # comparison is pam_env's OWN output, so any disagreement between this parser and a
  # different pam build moves the two values APART, never together. Over-stripping and
  # under-stripping both yield a non-passing verdict; neither can manufacture a VERIFIED.
  pin_strip_pam_quotes() {
    local v="$1" q
    case "$v" in
      '"'*) q='"' ;;
      "'"*) q="'" ;;
      *) printf '%s' "$v"; return 0 ;;
    esac
    v=${v#?}
    case "$v" in *"$q") v=${v%?} ;; esac
    printf '%s' "$v"
  }

  # Does the file ALREADY carry a pin line? Read ONCE, here, because two very different
  # boxes reach the FAILED verdict below and they need different remedies: a box with no
  # line (persist it) and a box whose line is present yet invisible to a fresh session
  # (a PAM condition — re-running with --yes finds the line already there and changes
  # NOTHING, so an operator told to do that just loops).
  #
  # AND IT CAPTURES THE VALUE, NOT JUST THE PRESENCE OF A LINE (#3414 roborev round 3).
  # THIS IS THE FOURTH INSTANCE OF THIS ISSUE'S OWN DEFECT IN THIS LANE, and it was inside
  # the correlation added to fix the third: presence is a PROXY for the fact that matters,
  # exactly as "the export is in ~/.bashrc" was. Concretely — the file holds
  # `CQLITE_GATE_MAX_CONCURRENCY=abc`, a sudoers env_file or ~/.pam_environment supplies
  # `1`, both halves of the file-AND-session conjunction are satisfied, the verdict is
  # VERIFIED — and every ordinary PAM session receives `abc`, which the gate discards for
  # its default formula and stamps N(invalid). The verdict must therefore compare VALUES.
  #
  # Parsed the way pam_env reads the file, because a second, subtly-different parser here
  # would be the same class of bug one layer down:
  #   * whole-line `#` comments are skipped;
  #   * NO inline-comment stripping — pam_env takes a trailing `# …` as part of the value,
  #     which is precisely why this section's own append puts its comment on its own line;
  #   * the LAST assignment wins if a file somehow carries two.
  # An assignment we cannot parse is UNMEASURED, never a mismatch: a parse failure is an
  # absence of evidence, and reporting it as a contradiction would invent one.
  PIN_FILE_HAS_LINE=unknown
  PIN_FILE_VALUE=""
  # pin_read_env_file: THE ONE PARSER, callable again (roborev job 319, Medium). It used to
  # run inline, exactly once, at the top of the section — so every later decision read a
  # snapshot taken before the writes. That was fine while this run was the only writer; it
  # stopped being fine when the create and append learned to LOSE a race (jobs 314/316),
  # because a lost race leaves the cache saying "no line" about a file that now HAS one, and
  # two things downstream act on that stale answer: the shell-profile fallback appends a
  # hardcoded `=1` — manufacturing the 1-vs-4 divergence 11ai exists to prevent — and the
  # verdict compares the session against an empty PIN_FILE_VALUE and reports NOT-SYSTEM-WIDE
  # about a correctly pinned box.
  #
  # Factored rather than re-implemented at the call sites: a second parse is a second place
  # for the sentinel/quote/last-wins rules to drift, and those rules are the subtle part.
  pin_read_env_file() {
  # Reset to the SAME default the one-shot version started from (:2166), not to empty: an
  # unreadable or symlinked file assigns nothing below, and `unknown` is the value the
  # "uncorrelatable file" verdict keys on. Resetting to "" silently disabled that branch —
  # caught by the existing case, which is the argument for making the function reproduce the
  # original initial state rather than an intuitive-looking blank one.
  PIN_FILE_HAS_LINE=unknown; PIN_FILE_VALUE=""
  if [ ! -e "$PIN_ENV_FILE" ]; then
    PIN_FILE_HAS_LINE=absent-file
  elif [ ! -L "$PIN_ENV_FILE" ] && [ -r "$PIN_ENV_FILE" ]; then
    if grep -Eq '^[[:space:]]*CQLITE_GATE_MAX_CONCURRENCY[[:space:]]*=' "$PIN_ENV_FILE" 2>/dev/null; then
      # `sed -n 's/…//p' | tail -1` = last assignment wins. The pattern anchors at line
      # start (so a `#`-commented line cannot match) and stops at the FIRST `=`, taking
      # everything after it verbatim — no trimming, no comment stripping.
      # A SENTINEL PREFIX, because an EMPTY capture and a FAILED capture are otherwise the
      # same string: `CQLITE_GATE_MAX_CONCURRENCY=` is a legitimate (empty, gate-invalid)
      # value, while sed producing nothing means the parse failed. Without the marker the
      # `unparseable` branch is unreachable and an unparseable file would silently report
      # an empty value — the unset-vs-set-empty conflation this issue removed from the
      # gate, re-created in the parser that checks it.
      pin_file_raw=$(sed -n 's/^[[:space:]]*CQLITE_GATE_MAX_CONCURRENCY[[:space:]]*=/VAL:/p' "$PIN_ENV_FILE" 2>/dev/null | tail -1)
      case "$pin_file_raw" in
        VAL:*) PIN_FILE_VALUE=$(pin_strip_pam_quotes "${pin_file_raw#VAL:}"); PIN_FILE_HAS_LINE=yes ;;
        *)     PIN_FILE_HAS_LINE=unparseable ;;
      esac
    else
      PIN_FILE_HAS_LINE=no
    fi
  fi
  }
  pin_read_env_file

  PIN_CREATE_RESIDUE=""
  # pin_append_env_file: CHECK-AND-APPEND under a lock, re-reading inside it (roborev job
  # 316, Medium). The caller's "is a line already there" fact is read ~150 lines earlier, and
  # the append was a bare `tee -a`, so a value added in between — a provisioner setting a
  # DELIBERATE `=4`, or a peer bootstrap on the same box — was silently overridden, because
  # pam_env takes the LAST assignment. That breaks this script's own stated contract that it
  # never rewrites an existing value, and two concurrent runs also produced DUPLICATE lines.
  #
  # `flock` serialises against every cooperating writer (both bootstrap runs take it), and
  # the re-read INSIDE the lock shrinks the window to the locked region. Exit 3 means
  # someone else won and we must not write.
  #
  # RESIDUAL, DECLARED: flock is ADVISORY, so a writer using a plain `>>` and no lock can
  # still interleave. That is not closable from here — it needs cooperation from the other
  # writer — and it is strictly better than the unlocked version it replaces. Where flock is
  # absent the same body runs unlocked, which still buys the re-read: losing the lock is a
  # weaker guarantee, not a reason to skip the check.
  pin_append_env_file() {
    local -a pin_lock=()
    command -v flock >/dev/null 2>&1 && pin_lock=(flock "$PIN_ENV_FILE")
    ${PIN_ROOT[@]+"${PIN_ROOT[@]}"} ${pin_lock[@]+"${pin_lock[@]}"} bash -c '
      f="$1"; comment="$2"; line="$3"
      if grep -Eq "^[[:space:]]*CQLITE_GATE_MAX_CONCURRENCY[[:space:]]*=" "$f" 2>/dev/null; then
        exit 3
      fi
      prefix=""
      # A file whose last byte is not a newline would otherwise have our KEY=VALUE welded
      # onto its final line, and pam_env would read the result as one malformed entry.
      if [ -s "$f" ] && [ -n "$(tail -c 1 "$f" 2>/dev/null)" ]; then prefix="
"; fi
      printf "%s%s\n%s\n" "$prefix" "$comment" "$line" >> "$f"
    ' _ "$PIN_ENV_FILE" "$PIN_ENV_COMMENT" "$PIN_ENV_LINE" 2>/dev/null
  }

  # pin_create_env_file: CREATE-IF-ABSENT, atomically (roborev job 314, Medium). The
  # `[ ! -e "$PIN_ENV_FILE" ]` test in the caller and the write are two separate steps, and
  # the write used to be a TRUNCATING `tee` — so a file created in between, by cloud-init, a
  # provisioning run or a peer agent on the same box, was silently OVERWRITTEN and whatever
  # it held was destroyed. On a fleet that runs bootstrap on several lanes per box that is a
  # live race, not a theoretical one.
  #
  # `set -C` (noclobber) opens with O_EXCL, so the KERNEL arbitrates and a loser writes
  # nothing — the same reason the claim protocol pushes a ref instead of checking for one.
  # `umask 022` establishes the mode AT CREATION instead of chmod-ing afterwards, which also
  # closes the window where the file briefly exists at a mode nobody chose. The readback in
  # The mode is VERIFIED, not merely instructed: setting a umask/chmod is an instruction, and
  # this section's whole rule is that an instruction is not a measurement. The verification now
  # happens on the STAGED file before `ln` (job 329) rather than on the destination after it.
  pin_create_env_file() {
    # THE CREATE AND THE WRITE ARE SEPARATE STEPS SO THEIR FAILURES ARE DISTINGUISHABLE
    # (roborev job 321, Medium). Collapsing them and then asking `[ -e "$FILE" ]` cannot tell
    # SOMEONE ELSE'S file from OUR OWN partially-written one — both exist — and the two
    # dispositions are opposite: leave someone else's alone, roll our own back. Getting it
    # wrong falsely blamed a concurrent writer AND left a truncated assignment that a later
    # run would read as an existing pin and decline to repair.
    #   exit 4 = exclusive create refused, i.e. the file already existed  -> LOST RACE
    #   exit 5 = we created it and the write then failed                  -> OUR partial file
    local pin_child_rc
    ${PIN_ROOT[@]+"${PIN_ROOT[@]}"} bash -c '
      umask 022
      f="$3"
      # ATOMIC CREATE *WITH CONTENT*, via a hard link (roborev job 323, Medium). The previous
      # form created the file exclusively and THEN wrote it, which reopened the very window it
      # was meant to close: another writer appending between the two steps had its content
      # erased by the truncating write, so the create was "atomic" only in the sense that
      # nothing else could CREATE it. Writing the content into a temporary in the SAME
      # directory and then `ln`-ing it into place makes the file appear complete or not at
      # all — `ln` fails if the destination exists, so exclusivity and completeness are the
      # same operation instead of two.
      t="$f.cqlite-pin.$$"
      rm -f "$t" 2>/dev/null
      printf "%s\n%s\n" "$1" "$2" > "$t" 2>/dev/null || { rm -f "$t" 2>/dev/null; exit 5; }
      # ESTABLISH AND VERIFY THE MODE ON THE *TEMP*, BEFORE LINKING (roborev job 329, Medium).
      # This used to be a post-`ln` read-back with a rollback that removed the DESTINATION BY
      # PATHNAME, which is a destructive race: `ln` succeeding proves the inode is ours AT
      # LINK TIME, not at REMOVE time, so a provisioner that unlinked and replaced the file in
      # between had ITS replacement deleted by our rollback. Verifying here makes the
      # destination correct BY CONSTRUCTION — `ln` links the inode, so it carries this mode
      # from the instant it appears — and removes the need for any rollback at all.
      chmod 0644 "$t" 2>/dev/null || true
      # PORTABLE MODE READ (roborev job 330, Medium). `stat -c` is GNU-only; BSD/macOS `stat`
      # rejects it. The PRODUCTION path is Linux-gated, but the TEST SEAM stubs `uname` to
      # simulate Linux while linking the HOST copy of `stat`, so on a macOS host this check would
      # fail-closed (exit 6) and the create would never happen — a break that the Linux-only
      # justification does not cover. GNU first, BSD fallback; `stat -f %Lp` is the BSD spelling.
      _pm=$(stat -c %a "$t" 2>/dev/null || stat -f %Lp "$t" 2>/dev/null)
      [ "$_pm" = 644 ] || { rm -f "$t" 2>/dev/null; exit 6; }
      ln "$t" "$f" 2>/dev/null || { rm -f "$t" 2>/dev/null; exit 4; }
      rm -f "$t" 2>/dev/null
    ' _ "$PIN_ENV_COMMENT" "$PIN_ENV_LINE" "$PIN_ENV_FILE" 2>/dev/null
    pin_child_rc=$?
    if [ "$pin_child_rc" = 4 ]; then pin_create_rc=3; return 3; fi
    if [ "$pin_child_rc" = 6 ]; then
      # Mode could not be established ON THE STAGED FILE, so nothing was ever linked. The
      # destination is untouched by construction — there is no rollback to get wrong.
      PIN_CREATE_RESIDUE="0644 could not be established on the staged file, so $PIN_ENV_FILE was never created; the temporary was removed by the child"
      pin_create_rc=1; return 1
    fi
    if [ "$pin_child_rc" != 0 ]; then
      # NEVER TOUCH THE DESTINATION ON A PRE-LINK FAILURE (roborev job 328, Medium). This
      # branch used to `rm -f "$PIN_ENV_FILE"` unconditionally, which was DESTRUCTIVE and
      # defeated the very no-clobber guarantee the temp+`ln` rewrite exists for: exit 5 means
      # the TEMP write failed, i.e. `ln` never ran and the destination was NEVER ours — so if
      # a concurrent provisioner created /etc/environment during our write, we deleted THEIR
      # file and called it our own cleanup.
      #
      # The child already removes its own temp on every failure path, and there is NO child
      # exit where `ln` SUCCEEDED and we then failed (a successful `ln` falls through to
      # rc=0). Therefore the parent never needs to remove the destination here, and the
      # correct disposition is: report, and touch NOTHING.
      #
      # AND THERE IS NO OTHER ROLLBACK LEFT TO GET WRONG. An earlier revision kept a post-`ln`
      # mode rollback here on the argument that `ln` proves the inode is ours — true at LINK
      # time, FALSE at REMOVE time, which is the destructive race job 329 found. The mode is
      # now verified on the staged file before linking, so no rollback exists at all.
      PIN_CREATE_RESIDUE="the staging write failed (rc $pin_child_rc) before $PIN_ENV_FILE was linked; nothing was written there and the temporary was removed by the child"
      pin_create_rc=1; return 1
    fi
    pin_create_rc=0; return 0
  }

  # pin_create_mode_ok WAS HERE AND IS DELETED (roborev job 329, Medium). It read the mode
  # back AFTER `ln` and, on a mismatch, removed the DESTINATION BY PATHNAME — a destructive
  # race, because `ln` proves the inode is ours at LINK time and not at REMOVE time. The mode
  # is now established and verified on the TEMP before linking, so the destination is correct
  # from the instant it exists and there is nothing to roll back. Removing the rollback
  # removes the race; narrowing it would only have made the window smaller.
  if [ "$PIN_PLATFORM_UNMANAGED" = 1 ]; then
    PIN_PERSIST_NOTE="not persisted (no PAM-read system-wide env file on $PLATFORM)"
    info "not touching $PIN_ENV_FILE on this $PLATFORM host — pam_env is a Linux mechanism and this platform does not consume that file, so writing it would change host state for nothing"
  elif [ ! -e "$PIN_ENV_FILE" ]; then
    # CREATE IT ON LINUX WHEN AUTHORISED (#3414 final roborev). Refusing here meant a
    # MINIMAL Linux install — where /etc/environment simply does not ship — could never be
    # repaired: --fix-gate-pin declined, the probe found no pin, and --strict failed that
    # box's onboarding FOREVER. A repair flag that cannot repair the one case it exists for
    # is the "red on correct input" shape inverted: the box is fixable and we refuse to fix
    # it. pam_env consumes the file once it exists, so creating it is the repair.
    #
    # Created with the ownership and mode pam_env expects and every other consumer assumes:
    # root:root 0644. Explicitly NOT 0600 — this file is read by PAM for every login
    # session, and a mode nothing else on the box uses is its own trap. Guarded the same way
    # the append is: only under an explicit authorisation, only with write privilege, and
    # only at the literal production path (the seam forces PIN_ROOT empty, and the invariant
    # below still refuses a privileged write to a non-production path).
    if [ "$AUTO_YES" != 1 ] && [ "$FIX_GATE_PIN" != 1 ]; then
      PIN_PERSIST_NOTE="no $PIN_ENV_FILE and no authorisation to create it"
      info "no $PIN_ENV_FILE on this $PLATFORM host — pass --yes or --fix-gate-pin and it will be CREATED (root:root 0644) so pam_env can consume it"
    elif [ "$PIN_WRITE_PRIV" != 1 ]; then
      PIN_PERSIST_NOTE="no $PIN_ENV_FILE and no privilege to create it ($PIN_PRIV_STATE)"
      warn "gate-pin: $PIN_ENV_FILE does not exist and this run cannot create it ($PIN_PRIV_STATE) — the pin was NOT persisted"
      info "create it as root:  printf '%s\n' '$PIN_ENV_COMMENT' '$PIN_ENV_LINE' > $PIN_ENV_FILE && chmod 0644 $PIN_ENV_FILE"
    elif pin_create_env_file; then
      PIN_FILE_HAS_LINE=yes; PIN_FILE_VALUE="$PIN_ENV_VALUE"
      # REPORT WHAT IT ACTUALLY IS, read back — not what the write intended. An earlier
      # draft of this line asserted "root:root 0644" unconditionally, which is false
      # wherever the write was unprivileged (the test seam forces PIN_ROOT empty, so the
      # file belongs to the invoking user). A message claiming an ownership it did not
      # establish is the same presence-for-fact substitution this section exists to remove,
      # in the section's own output.
      pin_made=$(ls -ld -- "$PIN_ENV_FILE" 2>/dev/null | awk '{print $1" "$3":"$4}')
      info "CREATED $PIN_ENV_FILE (${pin_made:-mode/owner unreadable}) carrying '$PIN_ENV_LINE' — pam_env reads it at session creation, so NEW sessions pick it up"
    elif [ "${pin_create_rc:-}" = 3 ]; then
      # RACED, and the contract wins — same disposition as the append's lost race.
      pin_read_env_file
      PIN_PERSIST_NOTE="not persisted (another writer created $PIN_ENV_FILE first)"
      info "$PIN_ENV_FILE was created by something else while this run was working — left EXACTLY as it is, and re-read, so the verdict and the profile decision below use what the file NOW says rather than the snapshot taken before the race"
    elif [ -n "$PIN_CREATE_RESIDUE" ]; then
      # THE FAILURE REPORT MUST MATCH THE FILESYSTEM (roborev job 311, Low). The write is
      # two steps — content then mode — so a `tee` that succeeded followed by a mode that
      # could not be established used to take this branch and say "NOT persisted" while
      # leaving a POPULATED file behind. The next run then reads a present
      # CQLITE_GATE_MAX_CONCURRENCY line, treats the pin as already persisted, and never
      # repairs the mode: one run's reported failure becomes the next run's silent success
      # at a permission nothing chose. So the residue is rolled back, and where it cannot
      # be, that is stated rather than folded into the generic message.
      PIN_PERSIST_NOTE="could not create $PIN_ENV_FILE ($PIN_CREATE_RESIDUE)"
      warn "gate-pin: could not create $PIN_ENV_FILE — the pin was NOT persisted ($PIN_CREATE_RESIDUE)"
    else
      PIN_PERSIST_NOTE="could not create $PIN_ENV_FILE"
      warn "gate-pin: could not create $PIN_ENV_FILE — the pin was NOT persisted"
    fi
  elif [ -L "$PIN_ENV_FILE" ]; then
    PIN_PERSIST_NOTE="$PIN_ENV_FILE is a SYMLINK"
    warn "gate-pin: $PIN_ENV_FILE is a SYMLINK — refusing a privileged write that would follow it; nothing was persisted"
  elif [ ! -r "$PIN_ENV_FILE" ]; then
    PIN_PERSIST_NOTE="$PIN_ENV_FILE is unreadable"
    warn "gate-pin: cannot read $PIN_ENV_FILE — cannot tell whether the pin is already there, so nothing was written (a blind append could duplicate or contradict an existing line)"
  elif [ "$PIN_FILE_HAS_LINE" = yes ] || [ "$PIN_FILE_HAS_LINE" = unparseable ]; then
    # `unparseable` counts as PRESENT for the append decision even though its value could
    # not be read: grep DID match a line, so appending would leave the file with two
    # assignments and pam_env would silently take the last one.
    info "$PIN_ENV_FILE already carries a CQLITE_GATE_MAX_CONCURRENCY line — left EXACTLY as it is (a box deliberately running >1 concurrent gate overrides the pin; bootstrap never rewrites an existing value)"
  elif [ "$AUTO_YES" != 1 ] && [ "$FIX_GATE_PIN" != 1 ]; then
    PIN_PERSIST_NOTE="not persisted (neither --yes nor --fix-gate-pin)"
    info "persist the pin:  bash scripts/bootstrap-agent-machine.sh --fix-gate-pin   (appends '$PIN_ENV_LINE' to $PIN_ENV_FILE; --yes does it too)"
  elif [ "$PIN_WRITE_PRIV" != 1 ]; then
    PIN_PERSIST_NOTE="no privilege to write $PIN_ENV_FILE ($PIN_PRIV_STATE)"
    warn "gate-pin: cannot write $PIN_ENV_FILE ($PIN_PRIV_STATE) — the pin was NOT persisted"
    info "add these two lines as root:  $PIN_ENV_COMMENT / $PIN_ENV_LINE"
  elif { [ "$PIN_EUID" = 0 ] || [ -z "$PIN_EUID" ] || [ "${#PIN_ROOT[@]}" -gt 0 ]; } \
       && [ "$PIN_ENV_FILE" != /etc/environment ]; then
    # INVARIANT, enforced rather than argued — and keyed on EFFECTIVE PRIVILEGE, not on
    # the presence of a sudo prefix (#3414 roborev round 5). `${#PIN_ROOT[@]} -gt 0` alone
    # was a proxy: under EUID 0 the array is empty and the write is privileged regardless,
    # so the old form could not fire on the one path where it mattered most. An unknown
    # EUID counts as privileged, because a guard that abstains is not a guard.
    PIN_PERSIST_NOTE="internal invariant refused the write"
    warn "gate-pin: INTERNAL — refusing a PRIVILEGED write to a non-production path ($PIN_ENV_FILE)"
  else
    # A file whose last byte is not a newline would otherwise have our KEY=VALUE welded
    # onto its final line, and pam_env would read the result as one malformed entry.
    # Prepend a newline in that case only; the append happens at most once per box, so
    # this can never accumulate blank lines.
    # Appended at the END, after any managed marker block the image owner put in the
    # file (this fleet's images carry a `# >>> agent-ami worker auth >>>` block), so
    # nothing already there is disturbed.
    pin_append_env_file; pin_append_rc=$?
    if [ "$pin_append_rc" = 3 ]; then
      # RACED, and the contract wins. Reported as the left-alone case rather than as a
      # failure: nothing is wrong with the box, and the file now holds someone else's
      # deliberate value.
      pin_read_env_file
      PIN_PERSIST_NOTE="not persisted (a concurrent writer added a CQLITE_GATE_MAX_CONCURRENCY line first)"
      info "$PIN_ENV_FILE gained a CQLITE_GATE_MAX_CONCURRENCY line while this run was working — left EXACTLY as it is, because appending ours would land LAST and pam_env takes the last assignment, silently overriding a value someone else chose"
    elif [ "$pin_append_rc" = 0 ]; then
      # BOTH halves of the cached parse must move together. Setting only HAS_LINE left
      # PIN_FILE_VALUE at its pre-write value (empty), so the value comparison below
      # compared the session against what the file said BEFORE the append and reported a
      # mismatch on a box this very run had just pinned correctly. Caught by 11q/11u.
      PIN_FILE_HAS_LINE=yes
      PIN_FILE_VALUE="$PIN_ENV_VALUE"
      info "appended '$PIN_ENV_LINE' to $PIN_ENV_FILE — PAM reads it at session creation, so NEW sessions pick it up with no reboot and no re-login"
    else
      PIN_PERSIST_NOTE="the append to $PIN_ENV_FILE failed"
      warn "gate-pin: the append to $PIN_ENV_FILE FAILED — the pin was NOT persisted"
      info "add these two lines as root:  $PIN_ENV_COMMENT / $PIN_ENV_LINE"
    fi
  fi

  # The shell-profile export is the FALLBACK for a box where no system-wide value could be
  # established — that is the only state in which it adds anything, and the branches below
  # are ordered to say so. It is NOT merely "interactive convenience": PAM applies
  # /etc/environment to interactive login shells too, so where a system-wide value exists
  # this file is redundant at best and an override at worst (see the skip branch).
  #
  # It is reported with `info`, NEVER `ok`, whichever branch runs: a profile line is
  # precisely the artifact whose presence certified nothing for months, and letting it
  # produce a success verdict anywhere would reintroduce #3414.
  PROFILE=""
  case "${SHELL:-}" in
    */zsh) PROFILE="$HOME/.zshrc" ;;
    */bash) PROFILE="$HOME/.bashrc" ;;
    *) PROFILE="${ENV:-$HOME/.profile}" ;;
  esac
  if [ -n "$PROFILE" ] && [ -f "$PROFILE" ] && grep -q 'CQLITE_GATE_MAX_CONCURRENCY' "$PROFILE" 2>/dev/null; then
    info "$PROFILE already carries the export — INTERACTIVE shells only (stock ~/.bashrc returns early for non-interactive ones), so it says nothing about the shell a gate runs in"
  elif [ "$PIN_FILE_HAS_LINE" = yes ]; then
    # SKIPPED, DELIBERATELY, when the system-wide file already carries a value (#3414
    # roborev round 4). The append hardcodes `=1`, so on a box deliberately pinned to 4
    # it would MANUFACTURE a divergence between two mechanisms on the same machine —
    # this issue's own subject, created by the tool that exists to remove it.
    #
    # Skipping rather than deriving the value, for a reason stronger than "the profile is
    # only convenience": /etc/environment is read by PAM at SESSION CREATION, which
    # applies to interactive login shells too, so on such a box the interactive shell
    # ALREADY receives the system-wide value — a profile export could only OVERRIDE it,
    # handing interactive shells 1 while every non-interactive one gets 4. Deriving the
    # value instead would agree today and go stale the moment someone edits
    # /etc/environment, manufacturing the same divergence later and more quietly.
    info "not touching $PROFILE — $PIN_ENV_FILE already sets CQLITE_GATE_MAX_CONCURRENCY=$PIN_FILE_VALUE, and PAM delivers that to interactive shells too; appending a hardcoded '=$PIN_ENV_VALUE' here could only override it"
  elif [ "$PIN_FILE_HAS_LINE" != no ] && [ "$PIN_FILE_HAS_LINE" != absent-file ]; then
    # UNREADABLE IS NOT ABSENT (#3414 roborev round 8). The skip above keys on a value
    # having been FOUND; this branch catches the states where we could not tell — an
    # unreadable file, or a line we could not parse. Appending the hardcoded `=1` there
    # would recreate exactly the divergence the skip exists to prevent, because the file we
    # could not read may already pin 4. An unmeasurable state must not inherit the
    # permissive branch: append only where absence was AFFIRMATIVELY established.
    info "not touching $PROFILE — could not determine what $PIN_ENV_FILE sets ($PIN_FILE_HAS_LINE), and appending a hardcoded '=$PIN_ENV_VALUE' could contradict a value that is already there"
  elif [ "$AUTO_YES" = 1 ] && [ -n "$PROFILE" ]; then
    if printf '%s\n' "$EXPORT_LINE" >>"$PROFILE" 2>/dev/null; then
      info "appended the export to $PROFILE — the FALLBACK for interactive shells on a box with no system-wide value; it is not the verdict, which comes from the session probe below"
    else
      info "could not append to $PROFILE (the interactive fallback; the system-wide pin is what a gate reads) — add by hand: $EXPORT_LINE"
    fi
  fi

  # pin_gate_source_for <value>: what the GATE will do with <value>, echoed as its own
  # source token (`pinned` / `default` / `invalid` / `clamped`); rc 1 if the gate could
  # not be consulted.
  #
  # ASK THE GATE, DO NOT RE-DERIVE ITS RULES HERE. A copy of the resolver would be a
  # SECOND IMPLEMENTATION, and a second implementation's correctness is only knowable by
  # differential testing against the original (CLAUDE.md's #3283 lesson, learned from a
  # bash port of a Go function that was tested against a MODEL of Go rather than Go). The
  # original is one `--cpu-budget` call away: measured 0.4s, and it exits before the
  # #1825 slot logic so it creates no run directory and takes no slot. If the gate cannot
  # be consulted the answer is UNKNOWN, never an assumed `pinned` — a positive verdict
  # requires an affirmative measurement.
  # Echoes "<source>:<resolved-N>" (e.g. `pinned:1`), or rc 1 if the gate could not be
  # consulted. BOTH halves are returned because the SOURCE TOKEN ALONE CANNOT TELL YOU THE
  # ORACLE WAS REASONING ABOUT YOUR VALUE (#3414 roborev round 4): `pinned` says only that
  # *something* was a valid pin, and the caller must check that the N the gate resolved is
  # the value we handed it.
  #
  # BASH_ENV AND ENV ARE SCRUBBED HERE TOO, and this is the same hole closed for the probe
  # in round 2, sitting one call site over. This launches a FRESH NON-INTERACTIVE bash,
  # which SOURCES $BASH_ENV before running anything — so on a box whose sudoers lacks
  # env_reset an inherited BASH_ENV could export a valid integer into the ORACLE's shell,
  # overriding the `CQLITE_GATE_MAX_CONCURRENCY="$v"` set on this very command line. A
  # persisted value of `abc` would then get a `(pinned)` answer from an oracle that never
  # saw `abc`. Fixing the probe and leaving the oracle open is fixing one instance of a
  # class; the value check above is the belt to this braces, and vice versa.
  # pin_canon_decimal: the gate's leading-zero normalisation, MIRRORED here — and that is a
  # SECOND IMPLEMENTATION, which this repo is right to be suspicious of (roborev job 333).
  # Two reasons it is the lesser evil, and the mechanism that keeps it honest:
  #   * The alternative the finding also offered — have the oracle echo its INPUT back — is a
  #     change to the `cpu-budget:` line's CONTRACT, which is a published gate surface that
  #     other readers parse. Widening it to serve one caller is the bigger coupling.
  #   * The drift check below exists to catch the oracle answering about a DIFFERENT value, so
  #     it cannot be expressed without comparing against our input; asking the oracle to
  #     canonicalise our input for us is circular.
  # ANTI-DRIFT IS A TEST, NOT A COMMENT: the suite runs the REAL gate for `08` and requires
  # bootstrap to reach VERIFIED, so if the gate's normalisation rule ever changes this reds.
  pin_canon_decimal() {
    local c="$1"
    case "$c" in
      ''|*[!0-9]*) printf '%s' "$c"; return 0 ;;
    esac
    while [ "${#c}" -gt 1 ] && [ "${c#0}" != "$c" ]; do c="${c#0}"; done
    printf '%s' "$c"
  }

  pin_gate_source_for() {
    local v="$1" line tok n
    [ -r "$GATE" ] || return 1
    line=$(bounded 30 env -u BASH_ENV -u ENV CQLITE_GATE_MAX_CONCURRENCY="$v" CQLITE_GATE_NO_NICE=1 \
             bash "$GATE" --cpu-budget 2>/dev/null | grep -E '^cpu-budget: ' | head -1)
    [ -n "$line" ] || return 1
    # The line is space-delimited key=value tokens; max-concurrency=N(source) is ONE of
    # them, which is exactly why that token carries no spaces.
    tok=$(printf '%s\n' "$line" | tr ' ' '\n' | sed -n 's/^max-concurrency=//p' | head -1)
    case "$tok" in
      *"("*")") n=${tok%%(*}; tok=${tok#*(}; printf '%s:%s' "${tok%)}" "$n" ;;
      *) return 1 ;;
    esac
  }

  # pin_scope_note: what VERIFIED does NOT cover. Printed with the verdict, not buried in
  # a doc, because an unqualified VERIFIED reads as "gates on this box are pinned" and the
  # probe cannot see a gate launched from a non-PAM parent (#3414 review B2).
  # pin_scope_note: what VERIFIED does and does NOT cover.
  #
  # A CHECK USED TO LIVE HERE AND WAS DELETED ON PURPOSE (#3414 round 7). It parsed
  # /etc/pam.d/{sshd,login} and downgraded a would-be VERIFIED when those stacks did not
  # appear to read this file. Do not helpfully reintroduce it. Four reasons it went:
  #   * it accumulated two defects in two review rounds, both invisible on the happy path
  #     (its result assigned inside `$( )` so an unreadable config silently passed; then a
  #     substring match on `pam_env.so` that a comment or another module's args satisfied);
  #   * IT WAS CONFIG INSPECTION STANDING IN FOR RUNTIME BEHAVIOUR — reading a file to
  #     infer what a session will receive is the proxy reasoning this whole section exists
  #     to remove, and it was only ever admissible because it could not create a pass;
  #   * what it approximated is measured DIRECTLY, every run, by the gate itself:
  #     `cpu-budget: max-concurrency=N(pinned)` is the actual resolved cap of the actual
  #     gate, not an inference about one;
  #   * the only remaining fix was to make anything unparseable WEAKEN — but a weaken is
  #     non-passing, non-passing fails --strict, and --strict is what verify.run uses, so a
  #     PAM layout we could not parse would have failed ONBOARDING for that box. Exactly
  #     one layout has ever been validated.
  # The residual it addressed is therefore DOCUMENTED, not measured, and the honest text
  # below says so. Settling it needs a runtime probe of the real launch path, not a better
  # parser.
  pin_scope_note() {
    # What the correlation DOES buy: the line is in the system-wide file pam_env reads in
    # every PAM stack (sshd, login, su, sudo), so the claim is not limited to the one
    # session type the probe could open. What it does NOT buy is a launch path with no
    # PAM in its ancestry at all — that residual is real and is stated, not implied.
    info "scope: measured through a PAM-created (sudo) session against the line in $PIN_ENV_FILE. Whether the service stacks a gate is actually launched from also read that file is NOT checked here — and a process tree created WITHOUT PAM (a systemd unit, a container entrypoint) never has it applied at all"
    info "scope: pam_env reads $PIN_ENV_FILE at SESSION CREATION, so this verdict is about FUTURE sessions. THIS shell, and every process already descended from it — including workers a launcher started before now — do NOT have the pin and will not until their sessions are recreated. A gate launched by such a worker still resolves the #1825 cap from the formula (#3728)"
    info "scope: VERIFIED asserts that the file SETS this value and a fresh session SEES that same value — it does NOT prove the file is where the session got it. If this box also sets CQLITE_GATE_MAX_CONCURRENCY to the same value from a sudoers env_file or ~/.pam_environment, an $PIN_ENV_FILE that no PAM stack actually loads would still read VERIFIED. Agreement is measured; provenance is not (#3728)"
    info "the authoritative per-run confirmation is the gate's own SUMMARY line:  cpu-budget: ... max-concurrency=N(pinned)   (N(default) there means that gate did not see the pin)"
    [ -n "$PIN_PROBE_SUBJECT_NOTE" ] && info "subject: $PIN_PROBE_SUBJECT_NOTE"
  }

  # pin_value_remedy: the remedy for a VISIBLE but NOT-HONOURED value. Shared by both
  # not-honoured branches so neither can silently lose it.
  pin_value_remedy() {
    if [ "$PIN_FILE_HAS_LINE" = yes ] && [ "$PIN_FILE_VALUE" != "$pin_probe_seen" ]; then
      # COMPARE BEFORE PRESCRIBING (roborev job 311, Low). The enclosing `case` dispatches
      # on the GATE's classification of what the SESSION saw, which says nothing about
      # where that value came from. So a box whose system file is CORRECT (`=1`) but whose
      # session is overridden by a sudoers env_file or ~/.pam_environment holding `abc`
      # lands here, and the unconditional branch below sent the operator to edit a file
      # that is already right — they find nothing wrong and re-run into the same verdict.
      # That is the #3414 defect one level down: a remedy keyed on a verdict rather than on
      # the fact that decides between two remedies.
      info "the bad value is NOT coming from $PIN_ENV_FILE — that file sets CQLITE_GATE_MAX_CONCURRENCY='$PIN_FILE_VALUE' while this session sees '$pin_probe_seen', so a sudo- or user-specific source (a sudoers env_file, ~/.pam_environment, a launcher-injected env) is OVERRIDING it"
      info "fix or remove THAT override — editing $PIN_ENV_FILE would change a value that is already being ignored"
    elif [ "$PIN_FILE_HAS_LINE" = yes ]; then
      # "A POSITIVE INTEGER" IS ADVICE AN OVERSIZED VALUE HAS ALREADY TAKEN (roborev job
      # 333). `99999999999999999999` IS a positive integer and is still refused, so the
      # unqualified remedy sends that operator to re-read a line they will find nothing
      # wrong with. The bound is stated where the remedy is, not only in the diagnosis.
      info "fix the VALUE (not the presence) — edit the CQLITE_GATE_MAX_CONCURRENCY line in $PIN_ENV_FILE to a positive decimal integer of at most 18 digits (a larger one is refused even though it is positive); bootstrap deliberately never rewrites an existing value"
    else
      info "the value is visible but is NOT coming from $PIN_ENV_FILE — find and fix whatever sets it (a systemd unit, the image, a launcher-injected env), then re-run"
    fi
  }

  # ---- (2) THE VERDICT: an affirmative probe of a fresh, profile-free session ----
  #
  # THE SCRUB IS THE LOAD-BEARING PART. Bootstrap normally runs inside a session that
  # already inherited the value, so an UNSCRUBBED probe returns it on a box where
  # nothing is persisted at all — the same false positive as the old profile grep, one
  # level up, and it would certify the very failure this section exists to catch.
  # `env -u` removes it from the process handed to sudo; sudoers' `Defaults env_reset`
  # is belt, not braces, since a box without env_reset would pass an inherited value
  # straight through.
  #
  # BASH_ENV AND ENV ARE SCRUBBED FOR THE SAME REASON, and they are the hole that
  # "belt, not braces" actually admits (#3414 review). A NON-INTERACTIVE bash SOURCES
  # $BASH_ENV before running its command — so on a box whose sudoers lacks env_reset an
  # inherited BASH_ENV survives into the probe, that file can `export
  # CQLITE_GATE_MAX_CONCURRENCY=1`, and the probe reports the box pinned with NOTHING in
  # /etc/environment. Scrubbing the variable while leaving the mechanism that can
  # re-inject it is not a scrub. (`ENV` is POSIX sh's equivalent, scrubbed with it.)
  #
  # NO `-i`, so no PROFILE file is read (`~/.bash_profile`, `~/.bashrc`, `/etc/profile`)
  # — which is the point, since those are exactly the files #3414 showed a gate never
  # reads. Note the claim is "no profile file", NOT "no file at all": with BASH_ENV and
  # ENV scrubbed the remaining sources are the session's own (pam_env's /etc/environment
  # and ~/.pam_environment, a sudoers env_file). Negative control, run by hand on the box
  # this was written on: a variable exported in the parent shell but absent from
  # /etc/environment reads UNSET through this probe, while the persisted pin reads 1.
  #
  # WHAT THIS PROBE DOES **NOT** COVER, stated here because the verdict text says it too
  # (#3414 review B2). It measures a PAM-CREATED session, because `sudo` is the only way
  # to create one unprivileged. A gate is NOT launched through sudo, and a process tree
  # created WITHOUT PAM — a systemd unit, a container entrypoint — never has
  # /etc/environment applied to it at all. So VERIFIED means "a PAM-created session on
  # this box sees a value the gate honours", never "every gate on this box is pinned".
  # The authoritative per-run confirmation is the gate's OWN `cpu-budget:
  # max-concurrency=N(pinned)` token, which reports what that gate actually resolved.
  #
  # The bound may degrade to SIGTERM-only (a `timeout` without --kill-after) — tolerated
  # HERE, unlike the §3b push probe, because this probe is LOCAL, NON-MUTATING and
  # `sudo -n` never prompts, so there is nothing for a wedged child to hold open. What
  # is NOT tolerated is running it UNBOUNDED: hanging the fleet's provisioning entry
  # point is worse than an unmeasured verdict.
  PIN_PROBE_BOUND=20
  if [ "$PIN_PRIV_STATE" = no-timeout-binary ] || [ -z "$TIMEOUT_BIN" ]; then
    warn "gate-pin: UNMEASURED (no timeout/gtimeout on PATH — refusing to run an UNBOUNDED session probe during bootstrap; NOTHING was probed)"
    info "install GNU coreutils so the probe can be bounded (macOS: brew install coreutils), then re-run"
  elif [ "$PIN_PRIV_STATE" = invoker-unresolvable ]; then
    warn "gate-pin: UNMEASURED ($PIN_PROBE_SUBJECT_NOTE — refusing to answer about the wrong user)"
    info "re-run as the agent account itself, which needs no sudo metadata to be trusted"
  elif [ "$PIN_PRIV_STATE" = no-identity ]; then
    warn "gate-pin: UNMEASURED ('id -un' reported no identity, so there is no user to open a probe session as — pin visibility is UNKNOWN, not ok)"
  elif [ "$PIN_PRIV_STATE" = no-sudo-binary ]; then
    warn "gate-pin: UNMEASURED (no 'sudo' on this box, so no fresh PAM session can be created — pin visibility is UNKNOWN, not ok)"
    info "check by hand:  env -u CQLITE_GATE_MAX_CONCURRENCY -u BASH_ENV -u ENV sudo -u \"\$(id -un)\" bash -c 'printf \"[%s]\\n\" \"\${CQLITE_GATE_MAX_CONCURRENCY-UNSET}\"'"
    info "(the scrub and the '-' — not ':-' — are load-bearing: without them an INHERITED value, or a set-but-EMPTY one, reads as a healthy pin, which is the defect this section exists to remove)"
  elif [ "$PIN_PRIV_STATE" = sudo-runas-denied ]; then
    warn "gate-pin: UNMEASURED (sudo will not open a session as '$PIN_SELF_USER' without a password, so no probe session could be created — pin visibility is UNKNOWN, not ok)"
    info "this needs only a session as YOURSELF, not root — authenticate once and re-run:  sudo -v && bash scripts/bootstrap-agent-machine.sh"
  else
    pin_probe_rc=0
    # TWO markers, because SET-BUT-EMPTY and UNSET are different facts with different
    # consequences — the gate DISCARDS an empty value for its default formula and stamps
    # `(invalid)`, which is a misconfigured box, not an unprovisioned one. `${VAR+1}`
    # separates them; `${VAR-}` alone cannot, and collapsing them here would put the
    # `:-` defect this issue removed from the gate back into the tool that verifies it.
    pin_probe_out=$(bounded "$PIN_PROBE_BOUND" env -u CQLITE_GATE_MAX_CONCURRENCY -u BASH_ENV -u ENV \
      sudo -n -u "$PIN_SELF_USER" \
      bash -c 'printf "cqlite-gate-pin-probe-set=%s\ncqlite-gate-pin-probe=%s\n" "${CQLITE_GATE_MAX_CONCURRENCY+1}" "${CQLITE_GATE_MAX_CONCURRENCY-}"' 2>/dev/null) || pin_probe_rc=$?
    # Anchored on our own markers at line start, and each value is read from the FIRST
    # matching line: the probe's stdout can also carry a shell's own noise, and a
    # verdict decided by an unanchored match would be decided by whatever else printed.
    pin_probe_set=$(printf '%s\n' "$pin_probe_out" | sed -n 's/^cqlite-gate-pin-probe-set=//p' | head -1)
    pin_probe_seen=$(printf '%s\n' "$pin_probe_out" | sed -n 's/^cqlite-gate-pin-probe=//p' | head -1)
    # THE VERDICT IS A CONJUNCTION OF TWO MEASUREMENTS, NEVER A FILE-STATE PRECEDENCE
    # THAT OVERRIDES THE PROBE (#3414, lead ruling). Written out because getting the
    # asymmetry backwards is easy and would silently undo an earlier ruling in this same
    # issue:
    #
    #   session sees value | /etc/environment line        | verdict
    #   -------------------|------------------------------|--------------------------
    #   NO                 | anything — present, absent,   | FAILED. "Not visible" is an
    #                      | unreadable, or no such file   | AFFIRMATIVE measurement;
    #                      |                              | nothing about the file can
    #                      |                              | rescue or worsen it. The file
    #                      |                              | state picks only the REMEDY
    #                      |                              | TEXT below, never the verdict.
    #   yes                | present                      | VERIFIED (still subject to the
    #                      |                              | gate-honours check)
    #   yes                | verified absent (readable)   | NOT-SYSTEM-WIDE
    #   yes                | unreadable / undeterminable  | UNMEASURED — the attribution
    #                      |                              | half genuinely was not measured
    #
    # THE RULE THIS ENCODES, which came up three separate times in #3414: AN UNMEASURABLE
    # HALF MAY ONLY EVER WEAKEN A POSITIVE CLAIM; IT MAY NEVER SOFTEN A NEGATIVE ONE.
    # UNMEASURED earns its place by blocking a VERIFIED we cannot support — not by
    # excusing a FAILED we have already established. Collapsing every unreadable-file case
    # to UNMEASURED would downgrade a real FAILED to "could not measure", which is the
    # discard-a-measurement error already ruled against for the unwritable-file case.
    if [ "$pin_probe_rc" = 124 ] || [ "$pin_probe_rc" = 137 ]; then
      warn "gate-pin: UNMEASURED (the probe exceeded its ${PIN_PROBE_BOUND}s bound and was killed — pin visibility is UNKNOWN, not ok)"
    elif ! printf '%s\n' "$pin_probe_out" | grep -q '^cqlite-gate-pin-probe-set='; then
      warn "gate-pin: UNMEASURED (the probe session produced no cqlite-gate-pin-probe-set= line, rc=$pin_probe_rc — pin visibility is UNKNOWN, not ok)"
    elif [ -n "$pin_probe_set" ]; then
      # VISIBLE. That is only HALF the question: a value the gate does not HONOUR is a
      # pin in name only, and certifying it here would be this issue's own shape one
      # level further out — presence of a VISIBLE value standing in for a value that has
      # EFFECT. So the gate is asked what it will actually do with it.
      pin_gate_out=$(pin_gate_source_for "$pin_probe_seen") || pin_gate_out=""
      pin_gate_src=${pin_gate_out%%:*}
      pin_gate_n=${pin_gate_out#*:}
      # A `pinned` token whose N is NOT the value we handed the oracle means the oracle
      # answered about something else — the BASH_ENV-pollution shape above, or any future
      # way the two could drift. Demote it to the same non-answer as an unconsultable gate
      # rather than trusting the suffix.
      # COMPARED AS CANONICAL DECIMALS, NOT AS RAW STRINGS (roborev job 333, Medium). The
      # gate NORMALISES a valid leading-zero pin (`08` -> `8(pinned)`), so a raw string
      # compare read `8` != `08` and demoted a CORRECTLY PERSISTED `08` to UNMEASURED —
      # `--strict` red on a properly pinned box, i.e. the guard that reds on correct input,
      # which is the guard agents learn to waive. Introduced by this branch's own octal fix:
      # normalisation was added to the gate and this comparison was not told about it.
      # Canonicalising BOTH sides keeps the drift check intact — an oracle answering about a
      # different value still differs after normalisation.
      if [ "$pin_gate_src" = pinned ] \
         && [ "$(pin_canon_decimal "$pin_gate_n")" != "$(pin_canon_decimal "$pin_probe_seen")" ]; then
        pin_gate_src=""
      fi
      case "$pin_gate_src" in
        pinned)
          # TWO AFFIRMATIVE HALVES, AND NEITHER SUFFICES ALONE (#3414 roborev round 2).
          # Scoping the TEXT to "a sudo session" while leaving the VERDICT an `ok` still
          # certified onboarding green: zero warnings => "All checks green." => verify.run
          # passes, on a box where the value might reach ONLY sudo sessions (a sudoers
          # `env_file`, `~/.pam_environment`) while every gate launched outside sudo gets
          # nothing. A verdict that passes while its own text says it might not hold is a
          # contradiction, not a caveat.
          #
          # So the probe is CORRELATED WITH THE FILE, using the read already taken to
          # decide the append — no second probe:
          #   file line + session sees it  => VERIFIED, and that is well-founded for ANY
          #     PAM-created session (sshd, login, su — pam_env reads /etc/environment in
          #     all of those stacks, not just sudo's), not merely for the one we opened.
          #   session sees it, no file line => the value comes from something sudo- or
          #     user-specific; it is NOT a system-wide pin. Non-passing.
          # File-only was the ORIGINAL #3414 defect and session-only is this finding, so
          # requiring both is the smallest honest verdict. Two fixes the reviewer offered
          # are deliberately NOT taken: "verify through the actual gate launch path"
          # (bootstrap cannot know that path) and "keep any sudo-scoped result
          # non-passing" (the probe is ALWAYS sudo-scoped, so that reds every box's
          # onboarding forever — an always-firing alarm is one people learn to waive,
          # which is the same reason the fleet does not just let verify red).
          # ON A PLATFORM WITH NO PAM-READ SYSTEM FILE, the file half does not exist and
          # asking for it would manufacture a permanent failure on a correctly-pinned host
          # — the red-on-correct-input shape refused elsewhere in this section. The
          # remaining question is still affirmative and still platform-independent: a fresh
          # scrubbed session sees a value, and the GATE HONOURS it. That is a narrower
          # claim than the Linux verdict and is worded as one; it is NOT an exemption, and
          # an unpinned host still lands in the non-passing branch below (#3414 final
          # roborev — the earlier `NOT-APPLICABLE` ok certified exactly that host).
          if [ "${PIN_PLATFORM_UNMANAGED:-0}" = 1 ]; then
            # NO SUCCESS VERDICT IS AVAILABLE HERE, SO NONE IS GIVEN (#3414 roborev round
            # 14). With no PAM-read system-wide file there is nothing to correlate the
            # session value against — and that correlation is exactly how the Linux path
            # tells a machine-wide pin from a sudo- or user-scoped one (a sudoers
            # `env_file`, `~/.pam_environment`). The problem is not merely unsolved here,
            # it is UNSOLVABLE with the machinery this section has.
            #
            # So every verdict that reports a state either over-claims or permanently reds:
            # an `ok` certifies a host whose ordinary gate processes may be unpinned, and a
            # FAILED asserts an absence nothing established. The honest third answer is that
            # the measurement could not be taken, cause named. "Could not measure" is a
            # different statement from "wrong", and it is the true one — so this is NOT the
            # red-on-correct-input shape refused elsewhere in this section.
            #
            # SECOND GUARD ON THIS BRANCH REPLACED RATHER THAN PATCHED, after the PAM
            # weaken-signal. Introduced round 11, amended round 13, defective again round
            # 14; the pre-committed trigger for that pattern is deletion, and it applies to
            # a verdict the LEAD introduced exactly as it applied to the PAM signal.
            warn "gate-pin: UNMEASURED (a fresh, profile-free session on this $PLATFORM host sees CQLITE_GATE_MAX_CONCURRENCY=$pin_probe_seen and the gate would HONOUR it — but this platform has no PAM-read system-wide file to compare it against, so a machine-wide pin cannot be told apart from a sudo- or user-scoped one that ordinary gate processes never see)"
            info "on this platform the per-run authority is the gate's own SUMMARY line:  cpu-budget: ... max-concurrency=N(pinned)   (N(default) means that gate did not see the pin)"
          else
          case "$PIN_FILE_HAS_LINE" in
            yes)
              # STRING equality on the raw effective value, deliberately not a numeric
              # one. `1`, `01` and `1 ` are different strings and the gate's own resolver
              # already treats them differently (a trailing space matches `*[!0-9]*` and
              # is discarded as invalid). Normalising here would be a SECOND classifier
              # free to disagree with the gate — the thing avoided by asking the gate what
              # it honours instead of re-deriving its rules.
              if [ "$PIN_FILE_VALUE" = "$pin_probe_seen" ]; then
                # Called DIRECTLY: see the note on the function — a substitution here
                # forks, and both of its result globals would be lost with the fork.
                ok "gate-pin: VERIFIED ($PIN_ENV_FILE sets CQLITE_GATE_MAX_CONCURRENCY=$PIN_FILE_VALUE AND a fresh PAM-created, profile-free session sees that SAME value, which the gate HONOURS verbatim — max-concurrency=$pin_probe_seen(pinned); this run's own value, BASH_ENV and ENV were scrubbed first)"
                pin_scope_note
              else
                warn "gate-pin: NOT-SYSTEM-WIDE ($PIN_ENV_FILE sets CQLITE_GATE_MAX_CONCURRENCY='$PIN_FILE_VALUE' but this session sees '$pin_probe_seen' — a sudo- or user-specific source is OVERRIDING the system-wide file, so ordinary PAM sessions get the file's value and the gate will act on THAT, not on the one measured here)"
                info "fix the VALUE in $PIN_ENV_FILE (bootstrap never rewrites an existing value), or remove the per-user/sudoers override so the two agree"
              fi
              ;;
            unparseable)
              warn "gate-pin: UNMEASURED (a fresh session sees CQLITE_GATE_MAX_CONCURRENCY=$pin_probe_seen and the gate would honour it, but the CQLITE_GATE_MAX_CONCURRENCY assignment in $PIN_ENV_FILE could not be PARSED, so it cannot be compared against what the session saw)"
              ;;
            unknown)
              # Half the evidence is unreadable, so the correlation cannot be made. Not a
              # pass: a positive verdict requires an affirmative measurement of BOTH halves.
              warn "gate-pin: UNMEASURED (a fresh session sees CQLITE_GATE_MAX_CONCURRENCY=$pin_probe_seen and the gate would honour it, but $PIN_ENV_FILE could not be READ, so it cannot be confirmed the value is a system-wide pin rather than a sudo- or user-specific one)"
              ;;
            *)
              warn "gate-pin: NOT-SYSTEM-WIDE (a fresh session sees CQLITE_GATE_MAX_CONCURRENCY=$pin_probe_seen and the gate would honour it, but there is NO CQLITE_GATE_MAX_CONCURRENCY line in $PIN_ENV_FILE — so it is reaching this session from a sudo- or user-specific source (a sudoers env_file, ~/.pam_environment) and gates launched outside that source get nothing)"
              if [ "$PIN_FILE_HAS_LINE" = absent-file ] && [ "$PIN_PLATFORM_UNMANAGED" = 1 ]; then
                # UNMANAGED PLATFORM only — on Linux the absent file is creatable, so the
                # remedy below is the true one (roborev job 332; see the absent-file arm
                # of the FAILED table for the full reasoning).
                info "this $PLATFORM host has no $PIN_ENV_FILE, so there is no system-wide file to correlate against — set CQLITE_GATE_MAX_CONCURRENCY=1 in this host's own session-startup mechanism (launchd/systemd/the image)"
              elif [ "$PIN_FILE_HAS_LINE" = absent-file ]; then
                info "fix:  bash scripts/bootstrap-agent-machine.sh --fix-gate-pin   (this $PLATFORM host has no $PIN_ENV_FILE yet — the flag CREATES it carrying '$PIN_ENV_LINE', which every PAM session reads; the per-user source stays as it is)"
              else
                info "fix:  bash scripts/bootstrap-agent-machine.sh --fix-gate-pin   (persists '$PIN_ENV_LINE' to $PIN_ENV_FILE, which every PAM session reads — the per-user source stays as it is)"
              fi
              ;;
          esac
          fi
          ;;
        invalid)
          # Its OWN verdict, not FAILED: the pin is present and visible, so "persist the
          # pin" is the wrong remedy and would send the operator to a file that already
          # has a line in it. What is wrong is the VALUE.
          # THE CAUSE IS NAMED FROM THE VALUE, because `invalid` covers more than it used
          # to (roborev job 333). This branch widened it to include a digit string too
          # large to represent, and the diagnosis still said "empty or non-numeric" — so an
          # oversized value was told it was non-numeric and handed a "use a positive
          # integer" remedy it already satisfied. A remedy the operator has already
          # complied with is worse than none: it stops them looking.
          case "$pin_probe_seen" in
            '') pin_invalid_why="it is EMPTY" ;;
            *[!0-9]*) pin_invalid_why="it is not a plain decimal integer" ;;
            *) pin_invalid_why="it is a decimal integer too large to use as a slot cap (more than 18 significant digits)" ;;
          esac
          warn "gate-pin: NOT-HONOURED (a fresh session sees CQLITE_GATE_MAX_CONCURRENCY='$pin_probe_seen', but the gate DISCARDS it — $pin_invalid_why — and falls back to the #1825 default formula, stamping max-concurrency=N(invalid))"
          pin_value_remedy
          ;;
        clamped)
          warn "gate-pin: NOT-HONOURED (a fresh session sees CQLITE_GATE_MAX_CONCURRENCY='$pin_probe_seen', but the gate silently raises it to 1, stamping max-concurrency=1(clamped) — the cap you asked for is not the cap you get)"
          pin_value_remedy
          ;;
        *)
          # Visibility WAS measured; honouring was not. Not a pass: the sole oracle for
          # the second half could not be consulted.
          warn "gate-pin: UNMEASURED (a fresh session sees CQLITE_GATE_MAX_CONCURRENCY='$pin_probe_seen', but $GATE could not be consulted to confirm the gate HONOURS that value — half the question is unanswered, which is not ok)"
          ;;
      esac
    else
      # NOT VISIBLE. Two different boxes, two different remedies — split on a fact we
      # already read above rather than printing one remedy and hoping.
      warn "gate-pin: FAILED (a fresh profile-free session does NOT see CQLITE_GATE_MAX_CONCURRENCY — every non-interactive gate on this box will resolve the #1825 cap from the default formula and admit co-tenants, #3414)"
      # The verdict is ALREADY emitted, unconditionally, above. What follows selects the
      # REMEDY only — two different boxes reach FAILED and need different next steps, but
      # no file state can turn this verdict into anything else (see the table above).
      case "$PIN_FILE_HAS_LINE" in
        yes)
          info "the pin IS in $PIN_ENV_FILE and a fresh session still does not see it — this is a PAM condition, NOT a missing pin"
          info "re-running with --yes / --fix-gate-pin will NOT help: either finds the line already present and changes nothing"
          info "check the session stack reads it:  grep -n pam_env /etc/pam.d/sudo /etc/pam.d/login /etc/pam.d/sshd   (each needs 'pam_env.so readenv=1')"
          info "and re-check by hand:  env -u CQLITE_GATE_MAX_CONCURRENCY -u BASH_ENV -u ENV sudo -u \"\$(id -un)\" bash -c 'printf \"[%s]\\n\" \"\${CQLITE_GATE_MAX_CONCURRENCY-UNSET}\"'"
          ;;
        absent-file)
          # No remedy that names a file this host does not have (the ruling on #3414's
          # residual 4): telling a Mac to re-run --yes to append to /etc/environment is
          # advice that cannot work on the box it is printed for.
          #
          # BUT THAT RULING IS ABOUT AN UNMANAGED PLATFORM, AND APPLYING IT TO EVERY
          # ABSENT FILE MADE IT FALSE ON LINUX (roborev job 332). "The file is missing" and
          # "this platform has no such file" are DIFFERENT STATES: on Linux the missing
          # file is CREATED by --fix-gate-pin (the persist path above says so in its own
          # message), so "nowhere to persist it" sent an operator on a minimal Linux
          # image to hand-edit systemd while a working remedy sat one flag away. A false
          # remedy costs more than a missing one, because it stops them looking.
          #
          # The VERDICT was already scoped on platform rather than on file presence (the
          # test suite asserts exactly that); this scopes the REMEDY the same way.
          if [ "$PIN_PLATFORM_UNMANAGED" = 1 ]; then
            info "this $PLATFORM host has no $PIN_ENV_FILE, so bootstrap has nowhere to persist it — set CQLITE_GATE_MAX_CONCURRENCY=1 in this host's own session-startup mechanism (launchd/systemd/the image), NOT in a shell profile"
          else
            info "fix:  bash scripts/bootstrap-agent-machine.sh --fix-gate-pin   (this $PLATFORM host has no $PIN_ENV_FILE yet — the flag CREATES it carrying '$PIN_ENV_LINE', and pam_env reads it at session creation; --yes does it too)"
          fi
          ;;
        *)
          [ -n "$PIN_PERSIST_NOTE" ] && info "nothing was persisted this run: $PIN_PERSIST_NOTE"
          info "fix:  bash scripts/bootstrap-agent-machine.sh --fix-gate-pin   (appends '$PIN_ENV_LINE' to $PIN_ENV_FILE; --yes does it too)"
          ;;
      esac
      info "the gate reports the same fact on its cpu-budget line as max-concurrency=N(default) instead of N(pinned)"
    fi
  fi
fi

# ---- 5b2. sccache cache-size cap (issue #3727) ----
# THE SAME DEFECT AS 5b, ONE VARIABLE OVER, and it was live on every box in the fleet.
# `.agent-ami/profile.yaml` DECLARES SCCACHE_CACHE_SIZE, and nothing ever persisted it:
# measured, the variable existed only inside the agent process the launcher created — a fresh
# PAM session (`sudo -i`) and `env -i bash -lc` both reported it UNSET — while SCCACHE_DIR
# beside it in the same profile block WAS in /etc/environment. So the fleet-effective cap was
# sccache's own 10 GiB default, on boxes provisioned to hold more, and no artifact said so.
#
# AND THERE IS A SECOND HALF 5b DOES NOT HAVE: THE VALUE IS READ BY THE SCCACHE *SERVER* AT
# STARTUP. It is therefore fixed by whichever process FIRST started the server and NO later
# client can change it — measured on this fleet: with a server already up, `--show-stats`
# reports the same `max_cache_size` whether the client exports 30G, 7G or nothing at all. So
# env-var VISIBILITY IS NOT THE VERDICT here; the authoritative reading is the RUNNING
# SERVER's max_cache_size, and a stale server is its own state with its own remedy
# (`sccache --stop-server`, never "edit the value").
#
# THE VALUE->BYTES MAP IS ASKED OF SCCACHE, NEVER REIMPLEMENTED (the rule 5b's
# pin_gate_source_for states: a copy of the resolver is a SECOND IMPLEMENTATION whose
# correctness is only knowable by differential testing against the original). Measured on
# sccache 0.17.0, and this grammar is exactly where a careful bash reimplementation goes wrong:
#     30G -> 30 GiB      30g -> 30 GiB      500M -> 500 MiB     1T -> 1 TiB
#     30GiB -> DISCARDED (10 GiB default)   30GB -> DISCARDED   "30 G" -> DISCARDED
#     abc/empty/-5G/21-digits -> DISCARDED  100 -> 100 *BYTES*  0G -> 0 bytes
# and there is NO diagnostic for a discard: with SCCACHE_LOG=debug the server logs only
# `Init disk cache with dir "...", size 10737418240`, i.e. the fallback, never a rejection.
#
# THE ORACLE STARTS NOTHING, WHICH IS A DEVIATION FROM THIS ISSUE'S OWN PLAN AND IS BASED ON A
# LATER MEASUREMENT. The plan called for a throwaway isolated SERVER (private port + private
# SCCACHE_DIR, stopped afterwards under an isolation assert). Measured afterwards: with no
# server reachable, `sccache --show-stats` DOES NOT START ONE — nothing listens afterwards, the
# private SCCACHE_DIR stays empty, and a following `--stop-server` reports "couldn't connect" —
# and it answers `max_cache_size` from the CLIENT's own resolution of SCCACHE_CACHE_SIZE. That
# client resolution reproduced the server-measured table above on all 11 points (30G, 30g,
# 30GiB, 30GB, 100, 1536M, 1T, 0G, abc, empty, 21 digits, unset). So the oracle is a bounded
# READ against a private port and a private, empty SCCACHE_DIR: no child process to own, no
# port to hold, and — the point — NO `--stop-server` CALL ANYWHERE IN THIS SECTION, so the
# production server cannot be stopped by a mistake here. The isolation assert survives and is
# still the load-bearing line: the reported cache_location must contain OUR probe directory,
# or the reading is a foreign server's and is discarded. (Matched against the RAW JSON: the
# payload escapes its inner quotes as `"Local disk: \"/dir\""`, so a
# `"cache_location":"[^"]*` pattern silently matches nothing and every reading then comes from
# whatever server answered — that mistake was made once while measuring this, and it produced
# three confident, wrong numbers.)
#
# THE PRODUCTION READING IS THE JSON, NOT THE TEXT LINE. `--show-stats` renders the cap for
# humans and the rendering is LOSSY: measured, 1536M (1610612736 bytes) prints as `2 GiB` and
# 1025M prints as `1 GiB`, so a text comparison would accept two genuinely different caps as
# equal. `--stats-format json` carries `max_cache_size` and `cache_size` as exact byte
# integers. Both sides of every comparison are BYTES; the only raw-string comparison in this
# section is file-value vs session-value, for the reason 5b states (`1`, `01` and `1 ` are
# different strings, and normalising here would be a second classifier).
hdr "sccache cache-size cap (SCCACHE_CACHE_SIZE, issue #3727)"

# The PRODUCTION persistence target, written as a LITERAL here (same rule as 5b: the
# privileged write can never name anything else).
SCC_ENV_FILE=/etc/environment
SCC_ENV_FILE_IS_SEAM=0
SCC_SECTION_OK=1
SCC_PERSIST_NOTE=""
# NO INLINE COMMENT ON THE VALUE LINE (pam_env takes a trailing `# ...` as part of the value) —
# the same rule 5b's PIN_ENV_COMMENT follows.
SCC_ENV_COMMENT='# cqlite: sccache object-cache size cap (issue #3727)'
# THE SINGLE SOURCE OF TRUTH FOR THE FLEET CAP. `.agent-ami/profile.yaml` carries the same
# literal for the ONBOARD build (its env reaches launcher-created processes, which is precisely
# why it was never enough on its own), and scripts/tests/test_bootstrap_agent_machine.sh asserts
# the two agree — two spellings of one number is drift, and a drift check is cheaper than a
# convention. MUST be a <digits>[KkMmGgTt] form: see the grammar table above.
# The value is DERIVED AS A BRACKET (see .agent-ami/profile.yaml for the measurement and both
# bounds). The shape guard below still applies to it: an unusable literal cannot reach
# /etc/environment — where it would be silently discarded by sccache AND, because this section
# never rewrites an existing value, would be permanent.
SCC_ENV_VALUE='50G'
SCC_ENV_LINE="SCCACHE_CACHE_SIZE=$SCC_ENV_VALUE"

# A SHAPE GUARD ON OUR OWN CONSTANT — deliberately NOT a classifier of anybody else's value.
# This decides only whether WE are willing to write OUR literal; what sccache would do with an
# arbitrary string is asked of sccache (the oracle below). Fail-closed: an unusable literal
# refuses the write and says so, rather than persisting a line that would be discarded with no
# diagnostic and never rewritten.
SCC_VALUE_USABLE=1
case "$SCC_ENV_VALUE" in
  *[!0-9KkMmGgTt]*|''|*[KkMmGgTt]*[KkMmGgTt]*|[KkMmGgTt]*) SCC_VALUE_USABLE=0 ;;
  *[0-9]) SCC_VALUE_USABLE=0 ;;   # a bare integer means BYTES — never what this fleet wants
esac

if [ "$SKIP_SCCACHE_CAP" = 1 ]; then
  warn "sccache-cap: OPT-OUT ($SKIP_SCCACHE_CAP_HOW) — the cap was NOT persisted and neither its visibility nor the running server's enforced cap was measured"
  info "this run cannot certify that the sccache server enforces the cap this fleet provisions; drop the opt-out to measure it"
  SCC_SECTION_OK=0
fi

# PLATFORM: /etc/environment + pam_env is a Linux mechanism, so the file half of the
# correlation does not exist elsewhere. Same posture as 5b, for the same reason: an
# `ok "NOT-APPLICABLE"` here would let --strict CERTIFY AN UNCAPPED HOST, which is this
# issue's own defect wearing a platform label. Non-Linux is UNMEASURED, never a success.
SCC_PLATFORM_UNMANAGED=0
if [ "$SCC_SECTION_OK" = 1 ] && [ "$PLATFORM" != linux ]; then
  SCC_PLATFORM_UNMANAGED=1
  info "sccache-cap: no PAM-read system-wide env file on this $PLATFORM host, so bootstrap does not MANAGE the cap here — it still MEASURES the running server's enforced cap below"
fi

# `$EUID` — bash's own readonly — NEVER a PATH-resolved `id`, and a nonnumeric value is
# UNKNOWN (treated as privileged, so an unreadable identity fails closed). Same reasoning as
# 5b at the corresponding line; resolved again here because this section must not depend on 5b
# having run (a --skip-gate-pin run leaves 5b's locals unset).
SCC_EUID="${EUID-}"
case "$SCC_EUID" in ''|*[!0-9]*) SCC_EUID="" ;; esac

if [ "$SCC_SECTION_OK" = 1 ] && [ -n "${CQLITE_BOOTSTRAP_ENV_FILE:-}" ] \
   && { [ "$SCC_EUID" = 0 ] || [ -z "$SCC_EUID" ]; }; then
  # The seam is REFUSED under root or unknown identity: it could otherwise steer a PRIVILEGED
  # write at an env-chosen path. Refused rather than made safe — see 5b's full argument.
  warn "sccache-cap: SKIPPED (CQLITE_BOOTSTRAP_ENV_FILE is set and this process is root or of unknown identity — refusing a seam that could steer a PRIVILEGED write at an env-chosen path)"
  info "the seam is for unprivileged self-tests only; run them as a normal user"
  SCC_SECTION_OK=0
fi

if [ "$SCC_SECTION_OK" = 1 ] && [ -n "${CQLITE_BOOTSTRAP_ENV_FILE:-}" ]; then
  # TEST-ONLY SEAM — fail-closed, with NO production fallback (the #3249 lesson: a test seam
  # that degrades to the real path certifies the real path by accident). Same four guards 5b
  # applies: inert without CQLITE_BOOTSTRAP_TEST_MODE=1, never the production path, never a
  # symlink, absolute only.
  if [ "${CQLITE_BOOTSTRAP_TEST_MODE:-0}" != 1 ]; then
    warn "sccache-cap: SKIPPED (CQLITE_BOOTSTRAP_ENV_FILE is set without CQLITE_BOOTSTRAP_TEST_MODE=1) — refusing to persist the cap at an env-chosen path"
    SCC_SECTION_OK=0
  else
    case "$CQLITE_BOOTSTRAP_ENV_FILE" in
      /etc/environment)
        warn "sccache-cap: SKIPPED (the test seam may not name the production /etc/environment)"
        SCC_SECTION_OK=0 ;;
      /*)
        if [ -L "$CQLITE_BOOTSTRAP_ENV_FILE" ]; then
          warn "sccache-cap: SKIPPED (the test seam path is a SYMLINK — a write would follow it)"
          SCC_SECTION_OK=0
        elif [ ! -d "$(dirname "$CQLITE_BOOTSTRAP_ENV_FILE")" ]; then
          warn "sccache-cap: SKIPPED (the test seam's parent directory does not exist)"
          SCC_SECTION_OK=0
        else
          SCC_ENV_FILE="$CQLITE_BOOTSTRAP_ENV_FILE"; SCC_ENV_FILE_IS_SEAM=1
        fi ;;
      *)
        warn "sccache-cap: SKIPPED (CQLITE_BOOTSTRAP_ENV_FILE must be an ABSOLUTE path)"
        SCC_SECTION_OK=0 ;;
    esac
  fi
fi

if [ "$SCC_SECTION_OK" = 1 ]; then
  # ---- (0) resources + cleanup, REGISTERED BEFORE ANYTHING IS CREATED ----
  # The only resource this section owns is a private, empty SCCACHE_DIR for the oracle: it
  # starts no server, so there is no child to reap and no port held (see the oracle note in
  # the header comment — that is why the plan's start-a-server design was replaced).
  SCC_PROBE_DIRS=()
  scc_cleanup() {
    local d
    for d in ${SCC_PROBE_DIRS[@]+"${SCC_PROBE_DIRS[@]}"}; do
      # Guarded on the path SHAPE, not on trust in the variable: an rm -rf driven by a
      # variable is worth one cheap invariant.
      case "$d" in
        /tmp/*|/var/tmp/*|"${TMPDIR:-/nonexistent}"/*) rm -rf -- "$d" 2>/dev/null || true ;;
      esac
    done
    SCC_PROBE_DIRS=()
  }
  # ONE EXIT trap exists per shell, and section 3's board probe installs one to restore gh's
  # active account. COMPOSED, not replaced: a bare `trap 'scc_cleanup' EXIT` here would
  # silently drop that restore and could leave the operator's gh account switched.
  # restore_board_account is defined unconditionally at top level and returns immediately
  # unless BOARD_SWITCHED=1, so naming it is safe on every path.
  trap 'scc_cleanup; restore_board_account' EXIT
  trap 'scc_cleanup; restore_board_account; exit 130' INT
  trap 'scc_cleanup; restore_board_account; exit 143' TERM
  trap 'scc_cleanup; restore_board_account; exit 129' HUP

  # Every SCCACHE_* the caller exported is scrubbed from the oracle's environment, plus
  # BASH_ENV/ENV (a non-interactive bash SOURCES $BASH_ENV, so leaving them would let an
  # inherited file re-inject the very variable being measured). Blanket-unset rather than a
  # named list: an inherited SCCACHE_GHA_ENABLED / SCCACHE_REDIS_* / SCCACHE_WEBDAV_* moves
  # the "isolated" probe's storage to a shared backend, and a new backend variable in a future
  # sccache would be missed by any list written today. `env` applies -u before assignments, so
  # the three values the probe sets afterwards survive the scrub.
  SCC_ENV_SCRUB=(-u BASH_ENV -u ENV)
  for scc_n in $(compgen -e 2>/dev/null || true); do
    case "$scc_n" in SCCACHE_*) SCC_ENV_SCRUB+=(-u "$scc_n") ;; esac
  done
  unset scc_n

  SCC_ORACLE_WHY=""
  # scc_bytes_for <outvar> [<value>]: the bytes sccache resolves <value> to; omit <value>
  # entirely (not empty — set-but-empty is a distinct, measured state) to measure sccache's own
  # compiled-in DEFAULT. rc 0 with <outvar> set, or rc 1 with SCC_ORACLE_WHY naming the cause.
  scc_bytes_for() {
    local __out="$1"; shift
    local __have=0 __v=""
    if [ "$#" -ge 1 ]; then __have=1; __v="$1"; fi
    eval "$__out="
    SCC_ORACLE_WHY=""
    have sccache || { SCC_ORACLE_WHY="no 'sccache' on PATH, so the value->bytes map cannot be asked of the tool that owns it"; return 1; }
    if [ -z "$TIMEOUT_BIN" ]; then
      SCC_ORACLE_WHY="no timeout/gtimeout on PATH — refusing to run an UNBOUNDED sccache probe during bootstrap"
      return 1
    fi
    local __dir __port __json __rc __try
    __dir=$(mktemp -d "${TMPDIR:-/tmp}/cqlite-sccache-probe.XXXXXX" 2>/dev/null) || {
      SCC_ORACLE_WHY="a private SCCACHE_DIR could not be created"; return 1; }
    SCC_PROBE_DIRS+=("$__dir")
    for __try in 0 1 2; do
      # A PRIVATE PORT IS REQUIRED even though nothing is started: without it the client
      # connects to the DEFAULT port, i.e. to the PRODUCTION server, and would report ITS cap
      # as the answer to a question about our value. A collision with some other server is
      # caught by the isolation assert below (its cache_location is not our directory), and
      # retried on a different port rather than trusted.
      __port=$(( 40000 + (($$ + __try * 997) % 20000) ))
      __rc=0
      if [ "$__have" = 1 ]; then
        __json=$(bounded 20 env "${SCC_ENV_SCRUB[@]}" SCCACHE_DIR="$__dir" \
          SCCACHE_SERVER_PORT="$__port" SCCACHE_CACHE_SIZE="$__v" \
          sccache --show-stats --stats-format json 2>/dev/null) || __rc=$?
      else
        __json=$(bounded 20 env "${SCC_ENV_SCRUB[@]}" SCCACHE_DIR="$__dir" \
          SCCACHE_SERVER_PORT="$__port" \
          sccache --show-stats --stats-format json 2>/dev/null) || __rc=$?
      fi
      if [ "$__rc" != 0 ] || [ -z "$__json" ]; then
        SCC_ORACLE_WHY="the isolated sccache probe produced no stats (rc $__rc)"
        continue
      fi
      # THE ISOLATION ASSERT — the single most important line in this section. Matched against
      # the RAW payload, because the JSON escapes the quotes inside cache_location.
      case "$__json" in
        *"$__dir"*) ;;
        *) SCC_ORACLE_WHY="the isolated probe was answered by a DIFFERENT sccache (its cache location is not the private probe directory), so its cap says nothing about our value"
           continue ;;
      esac
      local __hits __n
      __hits=$(printf '%s\n' "$__json" | grep -o '"max_cache_size":[0-9][0-9]*' 2>/dev/null)
      __n=$(printf '%s\n' "$__hits" | grep -c '^' 2>/dev/null)
      if [ -z "$__hits" ]; then
        SCC_ORACLE_WHY="the isolated probe's JSON carries no max_cache_size field"
        continue
      elif [ "$__n" != 1 ]; then
        SCC_ORACLE_WHY="the isolated probe's JSON carries $__n max_cache_size fields, so which one answers is ambiguous"
        continue
      fi
      eval "$__out=\${__hits##*:}"
      SCC_ORACLE_WHY=""
      return 0
    done
    return 1
  }

  SCC_RUN_CAP=""
  SCC_RUN_WHY=""
  SCC_RUN_STATE=""        # ok | no-server | unreadable
  SCC_SERVER_STARTED=0
  SCC_SESSION_DIR=""      # the measured session's SCCACHE_DIR, as reported BY that session
  SCC_SESSION_PORT=""     # ... and its SCCACHE_SERVER_PORT
  SCC_SESSION_SET=""      # were they set at all (`${VAR+1}`)? empty and unset are different facts

  # THE THING VERIFIED MUST BE THE THING GATES WILL CONTACT (issue #3727 roborev round 2, f2).
  # The value was read from a fresh, profile-free PAM session; the server probe and the start ran
  # in BOOTSTRAP's ambient environment — a different environment. sccache's routing is env-driven
  # (SCCACHE_SERVER_PORT selects WHICH server a client talks to, SCCACHE_DIR which cache it uses),
  # so an invoking shell that exports either of them made this section inspect — or START — one
  # server and report VERIFIED about a value that gates, launched from the measured session, would
  # take to ANOTHER server. Nothing hostile is needed: one exported variable in the caller's shell
  # is enough. That is *verifying one object and certifying another*, which is the defect class
  # this whole section exists to remove, so it cannot be left as a caveat.
  #
  # FIXED BY MOVING THE MEASUREMENT, NOT BY CAPTURING THE ENVIRONMENT (roborev offered both). Every
  # server read and the start now run INSIDE the same `sudo -n -u <self>` fresh session that
  # produced the value, so the routing they use IS the routing that session has — no set of
  # variables has to be enumerated, and there is no curated list to fall out of date when sccache
  # adds a routing variable. The alternative (capture the session's SCCACHE_* and re-apply them
  # here) requires knowing the FULL routing set to be correct, and a partial capture would produce
  # exactly the false VERIFIED being fixed.
  #
  # ONE THING IS DELIBERATELY *NOT* RUN IN THAT CONTEXT: the value->bytes oracle. Its whole point is
  # ISOLATION — a private port and a private, empty SCCACHE_DIR — because it asks "what does sccache
  # make of this string", a question about the string and not about this box's cache.
  #
  # The BINARY is resolved here, by absolute path, and passed in: sudo replaces PATH with its own
  # `secure_path`, so a name would be resolved against a different PATH than the one bootstrap
  # checked, and a box whose sccache sits outside secure_path would report UNMEASURED while its
  # gates work fine — red on correct input. The routing comes from the session; the binary identity
  # comes from us.
  SCC_SCCACHE_BIN="$(command -v sccache 2>/dev/null || true)"
  # The ROUTING variables are scrubbed alongside BASH_ENV/ENV: an SCCACHE_DIR or
  # SCCACHE_SERVER_PORT exported in the caller's shell must not leak into the session and then be
  # reported as that session's routing — the same substitution the value scrub prevents, one
  # variable over. What remains is the session's own routing (pam_env, plus a shell profile for a
  # login shell), which is what a gate launched from such a session gets.
  scc_session_run() {
    local __b="$1"; shift
    bounded "$__b" env -u BASH_ENV -u ENV -u SCCACHE_DIR -u SCCACHE_SERVER_PORT \
      sudo -n -u "$SCC_SELF_USER" "$@"
  }
  # scc_running_cap: the cap the RUNNING server is enforcing, in bytes — read INSIDE the measured
  # session (see the note on scc_session_run: the routing must be the session's, or the verdict is
  # about a server no gate will contact).
  #
  # THE SOUNDNESS OF THE WHOLE VERDICT IS IN TELLING A SERVER'S ANSWER FROM THE CLIENT'S OWN, AND
  # A NULL `cache_size` DOES NOT DO IT. Measured, in this order, and the second measurement
  # falsified the first design:
  #   * with NO server running, `--show-stats` answers max_cache_size from the CLIENT's env
  #     (SCCACHE_CACHE_SIZE=7G reads 7516192768 with nothing up) and reports `"cache_size":null`;
  #   * BUT A RUNNING SERVER WITH AN EMPTY CACHE ALSO REPORTS `"cache_size":null` — verified by
  #     starting a real server at 40G on a private port and reading it back: cap 42949672960,
  #     size null. The two payloads are otherwise IDENTICAL (a field-by-field diff differs only
  #     in the values themselves), so "null size" cannot mean "no server", and a check keyed on
  #     it reported UNMEASURED on a box whose server it had just correctly started.
  # So attribution is decided by a DIFFERENTIAL, which is affirmative rather than inferred: read
  # the cap twice, once with the ambient env and once with SCCACHE_CACHE_SIZE forced to a SENTINEL
  # whose bytes differ from the first reading. A RUNNING server ignores the client's env (measured:
  # a server started at 40G reports 42949672960 to a client with the variable unset), so the two
  # readings AGREE ⇒ the number is the server's. With no server, the client resolves our env, so
  # the second reading moves ⇒ the number is ours, not a cap in force. Two client calls, ~10 ms
  # each, no mutation, and it needs no knowledge of ports or process tables.
  scc_running_cap() {
    SCC_RUN_CAP=""; SCC_RUN_WHY=""; SCC_RUN_STATE=unreadable
    [ -n "$SCC_SCCACHE_BIN" ] || { SCC_RUN_WHY="no 'sccache' on PATH, so no running server can be read"; return 1; }
    if [ -z "$TIMEOUT_BIN" ]; then
      SCC_RUN_WHY="no timeout/gtimeout on PATH — refusing an UNBOUNDED sccache probe"
      return 1
    fi
    local json rc=0 hits n
    json=$(scc_session_run 20 env -u SCCACHE_CACHE_SIZE "$SCC_SCCACHE_BIN" \
      --show-stats --stats-format json 2>/dev/null) || rc=$?
    if [ "$rc" != 0 ] || [ -z "$json" ]; then
      SCC_RUN_WHY="'sccache --show-stats --stats-format json' produced no output (rc $rc)"
      return 1
    fi
    hits=$(printf '%s\n' "$json" | grep -o '"max_cache_size":[0-9][0-9]*' 2>/dev/null)
    n=$(printf '%s\n' "$hits" | grep -c '^' 2>/dev/null)
    if [ -z "$hits" ]; then
      SCC_RUN_WHY="the running server's JSON carries no max_cache_size field"
      return 1
    elif [ "$n" != 1 ]; then
      SCC_RUN_WHY="the running server's JSON carries $n max_cache_size fields, so which one is the cap is ambiguous"
      return 1
    fi
    SCC_RUN_CAP=${hits##*:}
    # ---- THE ATTRIBUTION DIFFERENTIAL (see the note above) ----
    # The sentinel is chosen so its bytes CANNOT equal the first reading: 7K = 7168 bytes, and
    # where the first reading is itself 7168 the sentinel moves to 9K. Without that guard, a box
    # whose SCCACHE_CACHE_SIZE happens to be 7K with NO server would produce two equal readings
    # and be reported as server-attributed — the one input that could invert this test.
    local sentinel=7K sentinel_bytes=7168 json2 rc2=0 cap2=""
    if [ "$SCC_RUN_CAP" = "$sentinel_bytes" ]; then sentinel=9K; sentinel_bytes=9216; fi
    json2=$(scc_session_run 20 env SCCACHE_CACHE_SIZE="$sentinel" "$SCC_SCCACHE_BIN" \
      --show-stats --stats-format json 2>/dev/null) || rc2=$?
    if [ "$rc2" != 0 ] || [ -z "$json2" ]; then
      SCC_RUN_CAP=""
      SCC_RUN_WHY="the attribution differential could not be taken (the second 'sccache --show-stats' produced no output, rc $rc2), so it cannot be established whether $hits is a RUNNING server's cap or this client's own resolution of SCCACHE_CACHE_SIZE"
      return 1
    fi
    cap2=$(printf '%s\n' "$json2" | grep -o '"max_cache_size":[0-9][0-9]*' 2>/dev/null | head -1)
    cap2=${cap2##*:}
    if [ -z "$cap2" ]; then
      SCC_RUN_CAP=""
      SCC_RUN_WHY="the attribution differential could not be taken (the second reading carries no max_cache_size), so it cannot be established whether the cap is a RUNNING server's or this client's own"
      return 1
    fi
    if [ "$cap2" != "$SCC_RUN_CAP" ]; then
      # The reading MOVED with the client's env, so the client is answering: nothing is running.
      # AFFIRMATIVE, not a catch-all — this is the one branch that KNOWS there is no server, which
      # is what licenses the starter below. Every other failure stays `unreadable`.
      SCC_RUN_WHY="no sccache server is answering: the reported cap MOVED when the client's SCCACHE_CACHE_SIZE was changed to $sentinel ($SCC_RUN_CAP -> $cap2), which is what a client resolving its OWN env does — a running server's answer does not move. So there is no cap IN FORCE to compare against"
      SCC_RUN_CAP=""
      SCC_RUN_STATE=no-server
      return 1
    fi
    SCC_RUN_STATE=ok
    return 0
  }

  # scc_start_server_for <value>: START the production sccache server under <value>.
  #
  # THIS IS THE FIX FOR A REAL PROVISIONING FAILURE, AND IT IS A FEATURE RATHER THAN A SOFTENED
  # VERDICT (issue #3727 roborev finding 2). A freshly launched box has no sccache server until
  # its first cached compilation, so `cache_size` is null, so the cap IN FORCE is genuinely
  # unmeasurable — and this section would report UNMEASURED and fail `--strict` on EVERY new box
  # immediately after correctly persisting the value. An alarm that fires on every new box is one
  # people learn to waive.
  #
  # The mechanism this whole section is about supplies the fix: the cap is fixed by whichever
  # process starts the server FIRST, and nothing later can change it. So bootstrap deliberately
  # BECOMES that first starter, under the value a fresh session sees — which is the value every
  # future gate on this box would have started it with. That makes the cap effective NOW instead
  # of at some later lane's whim, and it turns a fresh box into a verifiable box in one step.
  #
  # NOTE THE ASYMMETRY, WHICH IS DELIBERATE: WE START A SERVER THAT DOES NOT EXIST, AND WE NEVER
  # STOP ONE THAT DOES. A running server may have a peer lane's gate compiling against it right
  # now, and killing it mid-build to apply a cap is a fleet-hostile act — so a server enforcing a
  # different cap keeps its NOT-HONOURED verdict and the `sccache --stop-server` REMEDY, for a
  # human to run between gates.
  #
  # STARTED INSIDE THE MEASURED SESSION (roborev round 2, f2), with SCCACHE_CACHE_SIZE forced to
  # the value that session sees. So the server is created with the session's OWN routing — its
  # SCCACHE_DIR and SCCACHE_SERVER_PORT — which is the server gates launched from such a session
  # will contact, and it is created as the account those gates run as rather than as whoever ran
  # bootstrap. (That also removes a smaller defect noted before this finding: started ambiently, a
  # `sudo bootstrap` would have created the server on ROOT's cache directory.) The return code is
  # REPORTED but never trusted: the verdict comes from re-reading the server, because "start-server
  # exited 0" and "a server is running with this cap" are different claims — and sccache exits 0
  # when a server is already up.
  scc_start_server_for() {
    local v="$1" rc=0
    [ -n "$SCC_SCCACHE_BIN" ] || { SCC_START_WHY="no 'sccache' on PATH"; return 1; }
    if [ -z "$TIMEOUT_BIN" ]; then
      SCC_START_WHY="no timeout/gtimeout on PATH — refusing to start a server UNBOUNDED"
      return 1
    fi
    scc_session_run 30 env SCCACHE_CACHE_SIZE="$v" "$SCC_SCCACHE_BIN" \
      --start-server >/dev/null 2>&1 || rc=$?
    SCC_START_WHY="'sccache --start-server' exited $rc in the measured session"
    return 0
  }

  # ---- (1) the identity to probe, and write privilege ----
  # Both resolved the way 5b resolves them, and for the same reasons: `id -un` is a PATH lookup
  # that must map back to $EUID or it names the wrong account; under `sudo bash bootstrap` the
  # right subject is the INVOKING account (a validated, nonzero SUDO_UID that agrees with
  # SUDO_USER), never root; anything absent or inconsistent is UNMEASURED rather than a guess.
  SCC_PROBE_SUBJECT_NOTE=""
  SCC_PRIV_STATE=unknown
  SCC_ROOT=()
  SCC_SELF_USER=$(id -un 2>/dev/null || true)
  if [ -n "$SCC_SELF_USER" ]; then
    scc_name_uid=$(id -u "$SCC_SELF_USER" 2>/dev/null || echo none)
    if [ "$scc_name_uid" != "$SCC_EUID" ]; then
      SCC_PROBE_SUBJECT_NOTE="'id -un' answered '$SCC_SELF_USER', which resolves to uid $scc_name_uid rather than this process's EUID $SCC_EUID — a shadowed 'id' or a stale NSS mapping, so probing that name would answer about the wrong account"
      SCC_SELF_USER=""
    fi
  fi
  if [ "$SCC_EUID" = 0 ]; then
    scc_sudo_uid="${SUDO_UID-}"
    case "$scc_sudo_uid" in ''|*[!0-9]*) scc_sudo_uid="" ;; esac
    scc_sudo_name="${SUDO_USER-}"
    if [ -z "$scc_sudo_uid" ]; then
      SCC_SELF_USER=""
      SCC_PROBE_SUBJECT_NOTE="running as root with no usable SUDO_UID (absent or non-numeric), so the account a gate would run as is unknown"
    elif [ "$scc_sudo_uid" = 0 ]; then
      SCC_SELF_USER=""
      SCC_PROBE_SUBJECT_NOTE="SUDO_UID is 0, so sudo was invoked BY root — that tells us nothing about the account a gate runs as"
    else
      scc_resolved=$(id -un "$scc_sudo_uid" 2>/dev/null || true)
      if [ -z "$scc_resolved" ]; then
        SCC_SELF_USER=""
        SCC_PROBE_SUBJECT_NOTE="SUDO_UID $scc_sudo_uid does not resolve to an account"
      elif [ -n "$scc_sudo_name" ] && [ "$(id -u "$scc_sudo_name" 2>/dev/null || echo none)" != "$scc_sudo_uid" ]; then
        SCC_SELF_USER=""
        SCC_PROBE_SUBJECT_NOTE="INCONSISTENT sudo metadata — SUDO_USER '$scc_sudo_name' does not resolve to SUDO_UID $scc_sudo_uid, so which account to probe is ambiguous"
      else
        SCC_SELF_USER="$scc_resolved"
        SCC_PROBE_SUBJECT_NOTE="probed as '$scc_resolved' (uid $scc_sudo_uid, the account that invoked sudo), not root — root's session is not the one a gate runs in"
      fi
    fi
  fi
  if [ -z "$SCC_SELF_USER" ]; then
    SCC_PRIV_STATE=no-identity
    [ -n "$SCC_PROBE_SUBJECT_NOTE" ] && SCC_PRIV_STATE=invoker-unresolvable
  elif ! have sudo; then
    SCC_PRIV_STATE=no-sudo-binary
  elif [ -z "$TIMEOUT_BIN" ]; then
    # Checked BEFORE the first probe: `bounded` DEGRADES to running the command directly, so a
    # box with no timeout utility would otherwise hang bootstrap while the code claimed it
    # refuses unbounded session probing (5b's round-10 finding, same shape).
    SCC_PRIV_STATE=no-timeout-binary
  elif bounded 10 sudo -n -u "$SCC_SELF_USER" true >/dev/null 2>&1; then
    SCC_PRIV_STATE=sudo-nopasswd
  else
    SCC_PRIV_STATE=sudo-runas-denied
  fi
  # WRITE privilege is a SEPARATE question from session-probe privilege (5b's round-10 split):
  # a root shell with no sudo can persist but not probe; a narrowly-scoped sudoers rule can
  # probe but not persist. Neither is derived from the other.
  SCC_WRITE_PRIV=0
  if [ "$SCC_EUID" = 0 ]; then
    SCC_WRITE_PRIV=1; SCC_ROOT=()
  elif have sudo && [ -n "$TIMEOUT_BIN" ] && bounded 10 sudo -n true >/dev/null 2>&1; then
    SCC_WRITE_PRIV=1; SCC_ROOT=(sudo -n)
  fi
  # Test mode never runs a PRIVILEGED write; the sandbox file belongs to the invoking user.
  if [ "$SCC_ENV_FILE_IS_SEAM" = 1 ]; then SCC_ROOT=(); SCC_WRITE_PRIV=1; fi

  # ---- (2) what the FILE declares ----
  # pam_env's own de-quoting, reproduced so the value compared below is the value a session
  # actually RECEIVES: drop a leading `"`/`'` (whether or not it is closed), then a trailing one
  # of the SAME kind. Measured on this fleet's pam; see 5b's table for the full grid. A
  # disagreement with a different pam build moves the two compared values APART, never
  # together, so it can never manufacture a VERIFIED.
  scc_strip_pam_quotes() {
    local v="$1" q
    case "$v" in
      '"'*) q='"' ;;
      "'"*) q="'" ;;
      *) printf '%s' "$v"; return 0 ;;
    esac
    v=${v#?}
    case "$v" in *"$q") v=${v%?} ;; esac
    printf '%s' "$v"
  }

  SCC_FILE_HAS_LINE=unknown
  SCC_FILE_VALUE=""
  # THE ONE PARSER, callable again after a write or a lost race. Whole-line `#` comments are
  # skipped; NO inline-comment stripping (pam_env takes a trailing `# …` as part of the value,
  # which is why this section's own append puts its comment on its own line); the LAST
  # assignment wins. The SENTINEL PREFIX is what keeps an EMPTY value distinguishable from a
  # FAILED capture — `SCCACHE_CACHE_SIZE=` is a legitimate (and, measured, silently discarded)
  # value, while sed producing nothing means the parse failed.
  scc_read_env_file() {
    SCC_FILE_HAS_LINE=unknown; SCC_FILE_VALUE=""
    if [ ! -e "$SCC_ENV_FILE" ]; then
      SCC_FILE_HAS_LINE=absent-file
    elif [ ! -L "$SCC_ENV_FILE" ] && [ -r "$SCC_ENV_FILE" ]; then
      if grep -Eq '^[[:space:]]*SCCACHE_CACHE_SIZE[[:space:]]*=' "$SCC_ENV_FILE" 2>/dev/null; then
        scc_file_raw=$(sed -n 's/^[[:space:]]*SCCACHE_CACHE_SIZE[[:space:]]*=/VAL:/p' "$SCC_ENV_FILE" 2>/dev/null | tail -1)
        case "$scc_file_raw" in
          VAL:*) SCC_FILE_VALUE=$(scc_strip_pam_quotes "${scc_file_raw#VAL:}"); SCC_FILE_HAS_LINE=yes ;;
          *)     SCC_FILE_HAS_LINE=unparseable ;;
        esac
      else
        SCC_FILE_HAS_LINE=no
      fi
    fi
  }
  scc_read_env_file

  # ---- (3) persistence: append-if-absent / create-if-absent, NEVER a rewrite ----
  # Both mirror 5b's hardened forms: check-and-append under `flock` with the re-read INSIDE the
  # lock (a value added in between — a provisioner's deliberate cap, or a peer bootstrap — must
  # not be overridden, and pam_env takes the LAST assignment), and create via a staged temp
  # linked into place (`ln` fails if the destination exists, so exclusivity and completeness
  # are one operation) with the mode established by umask and VERIFIED on the temp before the
  # link. rc 3 = another writer won, and the contract wins with it.
  SCC_CREATE_RESIDUE=""
  scc_append_env_file() {
    local -a scc_lock=()
    command -v flock >/dev/null 2>&1 && scc_lock=(flock "$SCC_ENV_FILE")
    ${SCC_ROOT[@]+"${SCC_ROOT[@]}"} ${scc_lock[@]+"${scc_lock[@]}"} bash -c '
      f="$1"; comment="$2"; line="$3"
      if grep -Eq "^[[:space:]]*SCCACHE_CACHE_SIZE[[:space:]]*=" "$f" 2>/dev/null; then
        exit 3
      fi
      prefix=""
      if [ -s "$f" ] && [ -n "$(tail -c 1 "$f" 2>/dev/null)" ]; then prefix="
"; fi
      printf "%s%s\n%s\n" "$prefix" "$comment" "$line" >> "$f"
    ' _ "$SCC_ENV_FILE" "$SCC_ENV_COMMENT" "$SCC_ENV_LINE" 2>/dev/null
  }
  scc_create_env_file() {
    local scc_child_rc
    ${SCC_ROOT[@]+"${SCC_ROOT[@]}"} bash -c '
      umask 022
      f="$3"
      t="$f.cqlite-scc.$$"
      rm -f "$t" 2>/dev/null
      printf "%s\n%s\n" "$1" "$2" > "$t" 2>/dev/null || { rm -f "$t" 2>/dev/null; exit 5; }
      chmod 0644 "$t" 2>/dev/null || true
      # PORTABLE MODE READ: `stat -c` is GNU-only, `stat -f %Lp` is the BSD spelling.
      _pm=$(stat -c %a "$t" 2>/dev/null || stat -f %Lp "$t" 2>/dev/null)
      [ "$_pm" = 644 ] || { rm -f "$t" 2>/dev/null; exit 6; }
      ln "$t" "$f" 2>/dev/null || { rm -f "$t" 2>/dev/null; exit 4; }
      rm -f "$t" 2>/dev/null
    ' _ "$SCC_ENV_COMMENT" "$SCC_ENV_LINE" "$SCC_ENV_FILE" 2>/dev/null
    scc_child_rc=$?
    if [ "$scc_child_rc" = 4 ]; then scc_create_rc=3; return 3; fi
    if [ "$scc_child_rc" = 6 ]; then
      SCC_CREATE_RESIDUE="0644 could not be established on the staged file, so $SCC_ENV_FILE was never created"
      scc_create_rc=1; return 1
    fi
    if [ "$scc_child_rc" != 0 ]; then
      SCC_CREATE_RESIDUE="the staging write failed (rc $scc_child_rc) before $SCC_ENV_FILE was linked; nothing was written there"
      scc_create_rc=1; return 1
    fi
    scc_create_rc=0; return 0
  }

  # THE PLACEHOLDER / UNUSABLE-LITERAL REFUSAL. Persisting a value sccache would discard is
  # worse than persisting nothing: it is silent (there is no diagnostic anywhere), and because
  # this section never rewrites an existing value it would be permanent. The MEASUREMENT still
  # runs — a box someone else capped correctly can still reach VERIFIED.
  #
  # PLACED AT THE TWO WRITE SITES, NOT AT THE HEAD OF THE CHAIN. At the head it warned on every
  # box, including boxes that already carry a cap line and where nothing would be written — a
  # warning about a write that was never going to happen, which is noise in a section whose
  # every [warn] is supposed to be actionable (and, concretely, it would have added a warning to
  # every sandbox in this file's own self-test, the drift that has silently disabled cases here
  # four times).
  scc_refuse_literal() {
    SCC_PERSIST_NOTE="not persisted (the fleet cap literal '$SCC_ENV_VALUE' in this script is not a <digits>[KkMmGgTt] value)"
    warn "sccache-cap: this checkout's fleet cap literal is '$SCC_ENV_VALUE', which sccache would SILENTLY DISCARD (the accepted form is <digits>[KkMmGgTt], e.g. 30G; a bare integer means BYTES) — refusing to persist it, because this section never rewrites an existing value and a discarded line would be permanent and invisible"
    info "substitute the measured cap into SCC_ENV_VALUE in scripts/bootstrap-agent-machine.sh (and the matching literal in .agent-ami/profile.yaml), then re-run (issue #3727)"
  }
  if [ "$SCC_PLATFORM_UNMANAGED" = 1 ]; then
    SCC_PERSIST_NOTE="not persisted (no PAM-read system-wide env file on $PLATFORM)"
    info "not touching $SCC_ENV_FILE on this $PLATFORM host — pam_env is a Linux mechanism, so writing it would change host state for nothing"
  elif [ ! -e "$SCC_ENV_FILE" ]; then
    if [ "$AUTO_YES" != 1 ] && [ "$FIX_SCCACHE_CAP" != 1 ]; then
      SCC_PERSIST_NOTE="no $SCC_ENV_FILE and no authorisation to create it"
      info "no $SCC_ENV_FILE on this $PLATFORM host — pass --yes or --fix-sccache-cap and it will be CREATED (root:root 0644) so pam_env can consume it"
    elif [ "$SCC_VALUE_USABLE" != 1 ]; then
      scc_refuse_literal
    elif [ "$SCC_WRITE_PRIV" != 1 ]; then
      SCC_PERSIST_NOTE="no $SCC_ENV_FILE and no privilege to create it ($SCC_PRIV_STATE)"
      warn "sccache-cap: $SCC_ENV_FILE does not exist and this run cannot create it ($SCC_PRIV_STATE) — the cap was NOT persisted"
      info "create it as root:  printf '%s\n' '$SCC_ENV_COMMENT' '$SCC_ENV_LINE' > $SCC_ENV_FILE && chmod 0644 $SCC_ENV_FILE"
    elif scc_create_env_file; then
      SCC_FILE_HAS_LINE=yes; SCC_FILE_VALUE="$SCC_ENV_VALUE"
      scc_made=$(ls -ld -- "$SCC_ENV_FILE" 2>/dev/null | awk '{print $1" "$3":"$4}')
      info "CREATED $SCC_ENV_FILE (${scc_made:-mode/owner unreadable}) carrying '$SCC_ENV_LINE' — pam_env reads it at session creation, so NEW sessions pick it up"
    elif [ "${scc_create_rc:-}" = 3 ]; then
      scc_read_env_file
      SCC_PERSIST_NOTE="not persisted (another writer created $SCC_ENV_FILE first)"
      info "$SCC_ENV_FILE was created by something else while this run was working — left EXACTLY as it is, and re-read, so the verdict below uses what the file NOW says"
    else
      SCC_PERSIST_NOTE="could not create $SCC_ENV_FILE${SCC_CREATE_RESIDUE:+ ($SCC_CREATE_RESIDUE)}"
      warn "sccache-cap: could not create $SCC_ENV_FILE — the cap was NOT persisted${SCC_CREATE_RESIDUE:+ ($SCC_CREATE_RESIDUE)}"
    fi
  elif [ -L "$SCC_ENV_FILE" ]; then
    SCC_PERSIST_NOTE="$SCC_ENV_FILE is a SYMLINK"
    warn "sccache-cap: $SCC_ENV_FILE is a SYMLINK — refusing a privileged write that would follow it; nothing was persisted"
  elif [ ! -r "$SCC_ENV_FILE" ]; then
    SCC_PERSIST_NOTE="$SCC_ENV_FILE is unreadable"
    warn "sccache-cap: cannot read $SCC_ENV_FILE — cannot tell whether the cap is already there, so nothing was written (a blind append could duplicate or contradict an existing line)"
  elif [ "$SCC_FILE_HAS_LINE" = yes ] || [ "$SCC_FILE_HAS_LINE" = unparseable ]; then
    # `unparseable` counts as PRESENT: grep DID match a line, so appending would leave two
    # assignments and pam_env would silently take the last one.
    info "$SCC_ENV_FILE already carries a SCCACHE_CACHE_SIZE line — left EXACTLY as it is (a box deliberately running a different cap keeps it; bootstrap never rewrites an existing value)"
  elif [ "$AUTO_YES" != 1 ] && [ "$FIX_SCCACHE_CAP" != 1 ]; then
    SCC_PERSIST_NOTE="not persisted (neither --yes nor --fix-sccache-cap)"
    info "persist the cap:  bash scripts/bootstrap-agent-machine.sh --fix-sccache-cap   (appends '$SCC_ENV_LINE' to $SCC_ENV_FILE; --yes does it too)"
  elif [ "$SCC_VALUE_USABLE" != 1 ]; then
    scc_refuse_literal
  elif [ "$SCC_WRITE_PRIV" != 1 ]; then
    SCC_PERSIST_NOTE="no privilege to write $SCC_ENV_FILE ($SCC_PRIV_STATE)"
    warn "sccache-cap: cannot write $SCC_ENV_FILE ($SCC_PRIV_STATE) — the cap was NOT persisted"
    info "add these two lines as root:  $SCC_ENV_COMMENT / $SCC_ENV_LINE"
  elif { [ "$SCC_EUID" = 0 ] || [ -z "$SCC_EUID" ] || [ "${#SCC_ROOT[@]}" -gt 0 ]; } \
       && [ "$SCC_ENV_FILE" != /etc/environment ]; then
    # INVARIANT, enforced rather than argued, and keyed on EFFECTIVE PRIVILEGE rather than on
    # the presence of a sudo prefix (5b's round-5 finding): under EUID 0 the array is empty and
    # the write is privileged anyway. An unknown EUID counts as privileged.
    SCC_PERSIST_NOTE="internal invariant refused the write"
    warn "sccache-cap: INTERNAL — refusing a PRIVILEGED write to a non-production path ($SCC_ENV_FILE)"
  else
    scc_append_env_file; scc_append_rc=$?
    if [ "$scc_append_rc" = 3 ]; then
      scc_read_env_file
      SCC_PERSIST_NOTE="not persisted (a concurrent writer added a SCCACHE_CACHE_SIZE line first)"
      info "$SCC_ENV_FILE gained a SCCACHE_CACHE_SIZE line while this run was working — left EXACTLY as it is, because appending ours would land LAST and pam_env takes the last assignment"
    elif [ "$scc_append_rc" = 0 ]; then
      # BOTH halves of the cached parse move together: setting only HAS_LINE would leave the
      # comparison below reading the pre-write value and reporting a mismatch on a box this
      # very run just persisted correctly (5b's 11q/11u lesson).
      SCC_FILE_HAS_LINE=yes
      SCC_FILE_VALUE="$SCC_ENV_VALUE"
      info "appended '$SCC_ENV_LINE' to $SCC_ENV_FILE — PAM reads it at session creation, so NEW sessions pick it up with no reboot and no re-login"
    else
      SCC_PERSIST_NOTE="the append to $SCC_ENV_FILE failed"
      warn "sccache-cap: the append to $SCC_ENV_FILE FAILED — the cap was NOT persisted"
      info "add these two lines as root:  $SCC_ENV_COMMENT / $SCC_ENV_LINE"
    fi
  fi

  # NO SHELL-PROFILE FALLBACK, DELIBERATELY. 5b has one (reported with `info`, never `ok`); this
  # section does not, because a profile export is worth even less here: the sccache SERVER reads
  # the value at startup and is normally started by a gate running in a NON-interactive shell,
  # where a stock ~/.bashrc returns before reaching any export (#3414's own mechanism). Adding
  # an artifact that cannot affect the fact being measured is how #3727 happened.

  # ---- (4) THE VERDICT ----
  # scc_scope_note: what VERIFIED does NOT cover, printed with the verdict rather than buried in
  # a doc — an unqualified VERIFIED reads as "every gate on this box gets this cap".
  scc_scope_note() {
    info "scope: measured through TWO session types — a NON-LOGIN PAM session (sudo) and a LOGIN shell (sudo -i, which also runs /etc/profile.d) — and they AGREED on the value and on the routing; a disagreement would have been reported as CONFLICTING-SOURCES instead of this verdict. Whether the service stacks a gate is actually launched from read the same sources is NOT checked here, and a process tree created WITHOUT PAM (a systemd unit, a container entrypoint) never has $SCC_ENV_FILE applied at all — measured, such a session (env -i) sees the variable UNSET"
    info "scope: the cap is read by the sccache SERVER at STARTUP, so this verdict is about THIS box's CURRENT server plus FUTURE sessions. A server started later by a session that does NOT see the value will enforce sccache's default instead, and a server already running keeps its cap for its whole lifetime"
    info "scope: VERIFIED asserts that the file SETS this value, that a fresh session SEES that same value, and that the RUNNING server enforces exactly the bytes that value means — it does NOT prove the file is where the session got it. Agreement is measured; provenance is not"
    # WHICH SERVER WAS MEASURED, stated rather than assumed: every read and any start ran INSIDE
    # that session, so its routing decided which server answered. Printed even when unset, because
    # "sccache's own default location" is itself the answer a reader needs.
    case "$SCC_SESSION_SET" in
      *d*|*p*) info "scope: the server measured here is the one THAT SESSION routes to — SCCACHE_DIR='$SCC_SESSION_DIR' SCCACHE_SERVER_PORT='${SCC_SESSION_PORT:-<unset>}' (every read, and any start, ran inside that session, so this is the server a gate launched from such a session will contact)" ;;
      *)       info "scope: that session sets NEITHER SCCACHE_DIR NOR SCCACHE_SERVER_PORT, so the server measured here is the one at sccache's own default location and port (every read, and any start, ran inside that session)" ;;
    esac
    info "the authoritative per-run confirmation is the gate's own SUMMARY line:  accelerators: ... sccache-cap=<bytes>(pinned)   ((default) there means that gate's server is at sccache's own cap, (stale) that it predates the value)"
    if [ "${SCC_SERVER_STARTED:-0}" = 1 ]; then
      info "scope: there was NO server when this run began, so the cap is in force from now on because THIS RUN started it. Anything that stops the server later and does not see the value (a session created before the /etc/environment line existed) will start one at sccache's own default again"
    fi
    [ -n "$SCC_PROBE_SUBJECT_NOTE" ] && info "subject: $SCC_PROBE_SUBJECT_NOTE"
  }
  # scc_stale_remedy: the remedy for a visible, accepted value the RUNNING server does not
  # enforce. Split on the fact that decides between two remedies rather than on the verdict
  # (5b's job-311 lesson): if the file and the session disagree, sending the operator to edit
  # the file is advice about a value that is already being ignored.
  scc_stale_remedy() {
    if [ "${SCC_SERVER_STARTED:-0}" = 1 ]; then
      # Reached only if the server WE started reports a cap other than the bytes we started it
      # with — an sccache-level inconsistency, not a stale server, so the stop-server remedy
      # below would be nonsense advice. Named rather than folded into the generic text.
      info "note: this run STARTED that server itself under '$scc_probe_seen' and it STILL reports a different cap — that is an sccache-level inconsistency (the value was accepted by the isolated probe but not applied by the server), not a server predating the value; report it rather than restarting in a loop"
      return 0
    fi
    info "remedy:  sccache --stop-server    (the next compile restarts it, and the new server reads SCCACHE_CACHE_SIZE then). Do NOT edit the value — it is already correct and already visible; what is stale is the SERVER"
    if [ "$SCC_FILE_HAS_LINE" = yes ] && [ "$SCC_FILE_VALUE" != "$scc_probe_seen" ]; then
      info "note: the session value is NOT coming from $SCC_ENV_FILE — that file sets SCCACHE_CACHE_SIZE='$SCC_FILE_VALUE' while this session sees '$scc_probe_seen', so a sudo- or user-specific source is overriding it; fix that override too, or the two will keep disagreeing"
    fi
    info "on a box compiling right now, stopping the server costs the in-flight compile its cache connection — do it between gates, never during a peer lane's build"
  }
  # scc_fix_hint <what-the-flag-would-do>: the ONE place that prints the repair line, because
  # the repair is NOT `--fix-sccache-cap` while this checkout's cap literal is unusable — the
  # flag refuses that write (see the shape guard), so printing it would send an operator to run
  # a command that changes nothing. A remedy the operator has already complied with, or one
  # that cannot work, is worse than none: it stops them looking.
  scc_fix_hint() {
    if [ "$SCC_VALUE_USABLE" != 1 ]; then
      info "fix (TWO steps, #3727): FIRST substitute the measured cap for the '$SCC_ENV_VALUE' literal in SCC_ENV_VALUE (scripts/bootstrap-agent-machine.sh) and in .agent-ami/profile.yaml — it must be a <digits>[KkMmGgTt] form — THEN run:  bash scripts/bootstrap-agent-machine.sh --fix-sccache-cap"
      return 0
    fi
    info "fix:  bash scripts/bootstrap-agent-machine.sh --fix-sccache-cap   ($1)"
  }

  SCC_PROBE_BOUND=20
  if [ "$SCC_PRIV_STATE" = no-timeout-binary ] || [ -z "$TIMEOUT_BIN" ]; then
    warn "sccache-cap: UNMEASURED (no timeout/gtimeout on PATH — refusing to run an UNBOUNDED session probe during bootstrap; NOTHING was probed)"
    info "install GNU coreutils so the probe can be bounded (macOS: brew install coreutils), then re-run"
  elif [ "$SCC_PRIV_STATE" = invoker-unresolvable ]; then
    warn "sccache-cap: UNMEASURED ($SCC_PROBE_SUBJECT_NOTE — refusing to answer about the wrong user)"
  elif [ "$SCC_PRIV_STATE" = no-identity ]; then
    warn "sccache-cap: UNMEASURED ('id -un' reported no identity, so there is no user to open a probe session as — cap visibility is UNKNOWN, not ok)"
  elif [ "$SCC_PRIV_STATE" = no-sudo-binary ]; then
    warn "sccache-cap: UNMEASURED (no 'sudo' on this box, so no fresh PAM session can be created — cap visibility is UNKNOWN, not ok)"
    info "check by hand:  env -u SCCACHE_CACHE_SIZE -u BASH_ENV -u ENV sudo -u \"\$(id -un)\" bash -c 'printf \"[%s]\\n\" \"\${SCCACHE_CACHE_SIZE-UNSET}\"'"
  elif [ "$SCC_PRIV_STATE" = sudo-runas-denied ]; then
    warn "sccache-cap: UNMEASURED (sudo will not open a session as '$SCC_SELF_USER' without a password, so no probe session could be created — cap visibility is UNKNOWN, not ok)"
    info "this needs only a session as YOURSELF, not root — authenticate once and re-run:  sudo -v && bash scripts/bootstrap-agent-machine.sh"
  else
    # TWO SESSION TYPES, BECAUSE ONE BOX GAVE TWO ANSWERS (issue #3727, round 3 — MEASURED on
    # box3, and it is the root cause of the whole issue rather than a corner case):
    #     login shell   (sudo -n -u <self> -i ...)  -> 30G   from ~/.agent-ami/worker-env.sh
    #     non-login PAM (sudo -n -u <self>     ...)  -> 50G   from /etc/environment
    #     profile-free  (env -i bash -c ...)         -> UNSET
    # `/etc/profile.d/20-agent-ami.sh` SOURCES `~/.agent-ami/worker-env.sh`, and profile.d runs in
    # LOGIN shells AFTER pam_env has applied /etc/environment — so for a login shell the profile's
    # value OVERRIDES the file this section persists to. A single-session probe therefore cannot
    # answer "what cap will a gate get": the answer depends on how the gate was launched, and
    # certifying either value alone is the substitution this whole section exists to remove.
    # (SCCACHE_DIR escapes it only because both files happen to carry the SAME value.)
    #
    # THE SCRUB IS THE LOAD-BEARING PART of each probe: bootstrap normally runs inside a session
    # that already inherited the value, so an unscrubbed probe returns it on a box where nothing is
    # persisted — certifying the very failure this section exists to catch. `BASH_ENV`/`ENV` go
    # with it because a non-interactive bash SOURCES $BASH_ENV, which can re-export the variable
    # just scrubbed; the ROUTING variables go with it so the reported routing is the session's own.
    #
    # TWO markers per value, because SET-BUT-EMPTY and UNSET are different facts — measured,
    # sccache silently DISCARDS an empty value, which is a misconfigured box, not an unprovisioned
    # one.
    scc_probe_session() {
      local __mode="$1" __out="" __rc=0
      local -a __login=()
      [ "$__mode" = login ] && __login=(-i)
      scc_ps_rc=0; scc_ps_ok=0; scc_ps_set=""; scc_ps_seen=""
      scc_ps_routing=""; scc_ps_dir=""; scc_ps_port=""
      __out=$(bounded "$SCC_PROBE_BOUND" \
        env -u SCCACHE_CACHE_SIZE -u SCCACHE_DIR -u SCCACHE_SERVER_PORT -u BASH_ENV -u ENV \
        sudo -n -u "$SCC_SELF_USER" ${__login[@]+"${__login[@]}"} \
        bash -c 'printf "cqlite-scc-probe-set=%s\ncqlite-scc-probe=%s\ncqlite-scc-routing-set=%s%s\ncqlite-scc-dir=%s\ncqlite-scc-port=%s\n" "${SCCACHE_CACHE_SIZE+1}" "${SCCACHE_CACHE_SIZE-}" "${SCCACHE_DIR+d}" "${SCCACHE_SERVER_PORT+p}" "${SCCACHE_DIR-}" "${SCCACHE_SERVER_PORT-}"' 2>/dev/null) || __rc=$?
      scc_ps_rc=$__rc
      # Anchored on our own markers at line start: a LOGIN shell's stdout can also carry the
      # profile's own noise (a motd, an `echo` in someone's .bashrc), and a verdict decided by an
      # unanchored match would be decided by whatever else printed.
      scc_ps_set=$(printf '%s\n' "$__out" | sed -n 's/^cqlite-scc-probe-set=//p' | head -1)
      scc_ps_seen=$(printf '%s\n' "$__out" | sed -n 's/^cqlite-scc-probe=//p' | head -1)
      scc_ps_routing=$(printf '%s\n' "$__out" | sed -n 's/^cqlite-scc-routing-set=//p' | head -1)
      scc_ps_dir=$(printf '%s\n' "$__out" | sed -n 's/^cqlite-scc-dir=//p' | head -1)
      scc_ps_port=$(printf '%s\n' "$__out" | sed -n 's/^cqlite-scc-port=//p' | head -1)
      if [ "$__rc" != 124 ] && [ "$__rc" != 137 ] \
         && printf '%s\n' "$__out" | grep -q '^cqlite-scc-probe-set='; then
        scc_ps_ok=1
      fi
    }

    scc_probe_session nonlogin
    scc_probe_rc=$scc_ps_rc; scc_probe_ok=$scc_ps_ok
    scc_probe_set=$scc_ps_set; scc_probe_seen=$scc_ps_seen
    SCC_SESSION_SET=$scc_ps_routing; SCC_SESSION_DIR=$scc_ps_dir; SCC_SESSION_PORT=$scc_ps_port
    scc_probe_session login
    scc_login_rc=$scc_ps_rc; scc_login_ok=$scc_ps_ok
    scc_login_set=$scc_ps_set; scc_login_seen=$scc_ps_seen
    scc_login_routing=$scc_ps_routing; scc_login_dir=$scc_ps_dir; scc_login_port=$scc_ps_port

    # DISPLAY forms, built explicitly. An inline `${set:+'$seen'}${set:-<UNSET>}` is WRONG in a way
    # that reads fine: `:-` substitutes when the variable is EMPTY, so on a box where the value IS
    # set it printed the marker `1` after the quoted value. Two lines, no cleverness.
    scc_np_show="<UNSET>"; [ -n "$scc_probe_set" ] && scc_np_show="'$scc_probe_seen'"
    scc_lp_show="<UNSET>"; [ -n "$scc_login_set" ] && scc_lp_show="'$scc_login_seen'"
    if [ "$scc_probe_rc" = 124 ] || [ "$scc_probe_rc" = 137 ]; then
      warn "sccache-cap: UNMEASURED (the non-login session probe exceeded its ${SCC_PROBE_BOUND}s bound and was killed — cap visibility is UNKNOWN, not ok)"
    elif [ "$scc_probe_ok" != 1 ]; then
      warn "sccache-cap: UNMEASURED (the non-login probe session produced no cqlite-scc-probe-set= line, rc=$scc_probe_rc — cap visibility is UNKNOWN, not ok)"
    elif [ "$scc_login_ok" != 1 ]; then
      # THE LOGIN SHELL IS THE DOMINANT LAUNCH PATH ON THIS FLEET, so failing to measure it may
      # only WEAKEN a positive claim, never be skipped: an unmeasured half cannot license a
      # VERIFIED, and falling back to the non-login answer alone is exactly the single-session
      # certification this round removed.
      warn "sccache-cap: UNMEASURED (a fresh NON-LOGIN session sees SCCACHE_CACHE_SIZE=$scc_np_show, but the LOGIN-shell session could not be measured (rc=$scc_login_rc) — and on this fleet a login shell resolves the variable through /etc/profile.d, which OVERRIDES /etc/environment, so the answer for the launch path that matters is unknown)"
      info "check by hand:  sudo -n -u \"\$(id -un)\" -i bash -c 'printf \"[%s]\\n\" \"\${SCCACHE_CACHE_SIZE-UNSET}\"'"
    elif [ "$scc_probe_set" != "$scc_login_set" ] || [ "$scc_probe_seen" != "$scc_login_seen" ] \
         || [ "$SCC_SESSION_SET" != "$scc_login_routing" ] || [ "$SCC_SESSION_DIR" != "$scc_login_dir" ] \
         || [ "$SCC_SESSION_PORT" != "$scc_login_port" ]; then
      # CONFLICTING-SOURCES — RANKED ABOVE EVERY OTHER VERDICT, INCLUDING VERIFIED (round 3).
      # A box whose answer depends on HOW you ask has not been pinned, whatever any single probe
      # says, and a VERIFIED derived from one of two disagreeing sessions is a false
      # certification for the other one. So this is decided BEFORE the file/server correlation
      # and it never reaches an [ok].
      if [ "$scc_probe_seen" != "$scc_login_seen" ] || [ "$scc_probe_set" != "$scc_login_set" ]; then
        warn "sccache-cap: CONFLICTING-SOURCES (two session types a gate can be launched from resolve SCCACHE_CACHE_SIZE DIFFERENTLY — a non-login PAM session sees $scc_np_show while a LOGIN shell sees $scc_lp_show; whichever session starts the sccache server decides the cap, so this box is NOT pinned however either value was persisted)"
      else
        warn "sccache-cap: CONFLICTING-SOURCES (the two session types agree on the cap but NOT on the ROUTING — non-login: SCCACHE_DIR='$SCC_SESSION_DIR' SCCACHE_SERVER_PORT='${SCC_SESSION_PORT:-<unset>}'; login: SCCACHE_DIR='$scc_login_dir' SCCACHE_SERVER_PORT='${scc_login_port:-<unset>}' — so the two would contact DIFFERENT servers and a cap verified against one says nothing about the other)"
      fi
      info "on this fleet the login-shell value comes from a SHELL PROFILE: /etc/profile.d/20-agent-ami.sh SOURCES ~/.agent-ami/worker-env.sh, and profile.d runs AFTER pam_env has applied $SCC_ENV_FILE — so the profile WINS for any login shell, and persisting to $SCC_ENV_FILE alone cannot change what such a gate sees"
      info "find every source (the list is NOT exhaustive — a profile.d file may merely SOURCE another file, which is how this was missed once):  grep -rn SCCACHE_CACHE_SIZE $SCC_ENV_FILE /etc/profile.d ~/.agent-ami ~/.bashrc ~/.profile ~/.pam_environment 2>/dev/null"
      info "reconcile them to ONE value (this section never rewrites an existing value, so it will not choose for you); the durable fix for a launcher-provisioned box is .agent-ami/profile.yaml's env block, whose 4 keys match ~/.agent-ami/worker-env.sh exactly"
    elif [ -n "$scc_probe_set" ]; then
      # VISIBLE — which is only the FIRST of three facts. EFFECT is asked before ATTRIBUTION,
      # the same order 5b uses: a value with no effect makes the question of where it came from
      # moot, and the stale-server state is the one an operator must act on first.
      SCC_SEEN_BYTES=""; SCC_DEFAULT_BYTES=""
      if ! scc_bytes_for SCC_SEEN_BYTES "$scc_probe_seen"; then
        warn "sccache-cap: UNMEASURED (a fresh session sees SCCACHE_CACHE_SIZE='$scc_probe_seen', but what sccache would MAKE of that value could not be measured: $SCC_ORACLE_WHY)"
      elif ! scc_bytes_for SCC_DEFAULT_BYTES; then
        warn "sccache-cap: UNMEASURED (a fresh session sees SCCACHE_CACHE_SIZE='$scc_probe_seen' = $SCC_SEEN_BYTES bytes, but sccache's OWN default cap could not be measured, so an accepted value cannot be told from a silently discarded one: $SCC_ORACLE_WHY)"
      elif [ "$SCC_SEEN_BYTES" = "$SCC_DEFAULT_BYTES" ]; then
        # THE ONE AMBIGUITY, DECLARED RATHER THAN GUESSED. `30GiB` and `10G` both resolve to
        # sccache's default, so "accepted" and "silently discarded" are INDISTINGUISHABLE here
        # without reimplementing the grammar this section refuses to reimplement. A prefix
        # trick (probe `9$value` and see whether it moves) works and is a grammar inference in
        # disguise; it is deliberately not done. Operationally free: a cap EQUAL to sccache's
        # default caps nothing.
        warn "sccache-cap: UNMEASURED (a fresh session sees SCCACHE_CACHE_SIZE='$scc_probe_seen', which resolves to $SCC_SEEN_BYTES bytes — sccache's OWN default cap — so this check cannot tell an ACCEPTED value from a SILENTLY DISCARDED one)"
        info "sccache accepts <digits>[KkMmGgTt] with BINARY multipliers and silently discards everything else with NO diagnostic: measured, '30G' is 30 GiB while '30GiB' and '30GB' both fall back to the default, and a bare integer means BYTES"
        info "set a cap that DIFFERS from sccache's own default ($SCC_DEFAULT_BYTES bytes) and this becomes measurable — a cap equal to the default caps nothing anyway"
      else
        # ---- (3) THE CAP IN FORCE — and where there is NONE, become the first starter ----
        # See scc_start_server_for for why this is the fix rather than a softened verdict, and
        # for the start-but-never-stop asymmetry. Gated on an explicit authorisation, exactly as
        # the /etc/environment write is: a default `bootstrap` run mutates no host state, and
        # `.agent-ami/profile.yaml`'s verify.run passes --fix-sccache-cap, so the provisioning
        # path this exists for gets it.
        scc_run_ok=0
        SCC_START_WHY=""
        if scc_running_cap; then
          scc_run_ok=1
        elif [ "$SCC_RUN_STATE" = no-server ]; then
          if [ "$AUTO_YES" = 1 ] || [ "$FIX_SCCACHE_CAP" = 1 ]; then
            info "no sccache server is running on this box, so this run STARTS one under the value a fresh session sees ('$scc_probe_seen') — the cap is fixed by whichever process starts the server first, so being that process is what makes the cap effective NOW rather than whenever some later lane happens to compile"
            scc_start_server_for "$scc_probe_seen"
            if scc_running_cap; then
              scc_run_ok=1; SCC_SERVER_STARTED=1
              info "STARTED the sccache server ($SCC_START_WHY); it now reports a cap of $SCC_RUN_CAP bytes"
            fi
          fi
        fi
        if [ "$scc_run_ok" != 1 ]; then
          warn "sccache-cap: UNMEASURED (a fresh session sees SCCACHE_CACHE_SIZE='$scc_probe_seen' = $SCC_SEEN_BYTES bytes, but the cap actually IN FORCE could not be read: $SCC_RUN_WHY${SCC_START_WHY:+; a start was attempted — $SCC_START_WHY})"
          info "the value is read by the sccache SERVER at startup, so nothing about a visible variable establishes the cap in force — that is why this half is not assumed"
          if [ "$SCC_RUN_STATE" = no-server ] && [ "$AUTO_YES" != 1 ] && [ "$FIX_SCCACHE_CAP" != 1 ]; then
            info "nothing is running to measure, and a default run does not start one:  bash scripts/bootstrap-agent-machine.sh --fix-sccache-cap   starts the server under the persisted value and verifies the cap it enforces (--yes does it too)"
          fi
        elif [ "$SCC_RUN_CAP" != "$SCC_SEEN_BYTES" ]; then
          warn "sccache-cap: NOT-HONOURED (a fresh session sees SCCACHE_CACHE_SIZE='$scc_probe_seen' = $SCC_SEEN_BYTES bytes, but the RUNNING sccache server enforces $SCC_RUN_CAP bytes — it was started by a process that did not see this value, and sccache reads the cap ONCE, at server startup)"
          scc_stale_remedy
        elif [ "$SCC_PLATFORM_UNMANAGED" = 1 ]; then
          # No PAM-read system-wide file exists to correlate against, so a machine-wide cap
          # cannot be told from a sudo- or user-scoped one. No verdict that reports a state is
          # available here, so none is given (5b's round-14 ruling, same shape).
          warn "sccache-cap: UNMEASURED (a fresh, profile-free session on this $PLATFORM host sees SCCACHE_CACHE_SIZE='$scc_probe_seen' and the running server enforces exactly that ($SCC_RUN_CAP bytes) — but this platform has no PAM-read system-wide file to compare it against, so a machine-wide cap cannot be told apart from a sudo- or user-scoped one that ordinary gate processes never see)"
          info "on this platform the per-run authority is the gate's own SUMMARY line:  accelerators: ... sccache-cap=<bytes>(pinned)"
        else
          case "$SCC_FILE_HAS_LINE" in
            yes)
              # STRING equality on the raw effective value, deliberately not a numeric or
              # unit-aware one: `30G`, `030G` and `30G ` are different strings and only sccache
              # decides what each means. Both are already reduced to BYTES for the effect check
              # above; this half asks a different question — is the session getting THIS FILE's
              # value — and normalising here would be a second classifier free to disagree.
              if [ "$SCC_FILE_VALUE" = "$scc_probe_seen" ]; then
                # WHERE THIS RUN STARTED THE SERVER ITSELF, SAY SO IN THE VERDICT. The third
                # fact is then true because bootstrap MADE it true — a provisioning action, not
                # an independent observation of a server somebody else started — and a reader
                # who cannot tell those apart would over-read the [ok]. (The read-back is still
                # a measurement: it is sccache reporting what it actually enforces.)
                # ONE `ok` CALL, with the varying phrase in a variable: the self-test's
                # structural guard requires section 5b2 to contain exactly one `ok "` and for it
                # to be the named VERIFIED verdict, and two call sites would defeat a guard that
                # exists to stop a future `ok` sneaking in for a file write or an exemption.
                if [ "$SCC_SERVER_STARTED" = 1 ]; then
                  scc_srv_phrase="the sccache server THIS RUN STARTED under it enforces exactly the $SCC_SEEN_BYTES bytes it means — there was no server before, so this run became the first starter rather than leaving the cap to whichever lane compiled next"
                else
                  scc_srv_phrase="the ALREADY-RUNNING sccache server enforces exactly the $SCC_SEEN_BYTES bytes it means"
                fi
                ok "sccache-cap: VERIFIED ($SCC_ENV_FILE sets SCCACHE_CACHE_SIZE=$SCC_FILE_VALUE, a fresh PAM-created, profile-free session sees that SAME value, and $scc_srv_phrase; this run's own value, BASH_ENV and ENV were scrubbed first)"
                scc_scope_note
              else
                warn "sccache-cap: NOT-SYSTEM-WIDE ($SCC_ENV_FILE sets SCCACHE_CACHE_SIZE='$SCC_FILE_VALUE' but this session sees '$scc_probe_seen' — a sudo- or user-specific source is OVERRIDING the system-wide file, so ordinary PAM sessions get the file's value and whichever of them starts the sccache server will cap it with THAT, not with the one measured here)"
                info "fix the VALUE in $SCC_ENV_FILE (bootstrap never rewrites an existing value), or remove the per-user/sudoers override so the two agree"
              fi
              ;;
            unparseable)
              warn "sccache-cap: UNMEASURED (a fresh session sees SCCACHE_CACHE_SIZE='$scc_probe_seen' and the running server enforces exactly that, but the SCCACHE_CACHE_SIZE assignment in $SCC_ENV_FILE could not be PARSED, so it cannot be compared against what the session saw)"
              ;;
            unknown)
              warn "sccache-cap: UNMEASURED (a fresh session sees SCCACHE_CACHE_SIZE='$scc_probe_seen' and the running server enforces exactly that, but $SCC_ENV_FILE could not be READ, so it cannot be confirmed the value is a system-wide cap rather than a sudo- or user-specific one)"
              ;;
            *)
              warn "sccache-cap: NOT-SYSTEM-WIDE (a fresh session sees SCCACHE_CACHE_SIZE='$scc_probe_seen' and the running server enforces exactly the $SCC_SEEN_BYTES bytes it means, but there is NO SCCACHE_CACHE_SIZE line in $SCC_ENV_FILE — so it is reaching this session from a sudo- or user-specific source (a sudoers env_file, ~/.pam_environment, a launcher-injected env) and a server started outside that source gets sccache's default)"
              if [ "$SCC_FILE_HAS_LINE" = absent-file ]; then
                scc_fix_hint "this host has no $SCC_ENV_FILE yet — the flag CREATES it carrying '$SCC_ENV_LINE'; the per-user source stays as it is"
              else
                scc_fix_hint "persists '$SCC_ENV_LINE' to $SCC_ENV_FILE, which every PAM session reads — the per-user source stays as it is"
              fi
              ;;
          esac
        fi
      fi
    else
      # NOT VISIBLE. An affirmative measurement, so no file state can rescue or worsen it — the
      # file state selects the REMEDY only (5b's table, same rule: an unmeasurable half may only
      # ever weaken a POSITIVE claim, never soften a negative one).
      warn "sccache-cap: FAILED (a fresh profile-free session does NOT see SCCACHE_CACHE_SIZE — a gate launched from such a session starts the sccache server with sccache's own default cap, whatever this fleet provisions, #3727)"
      case "$SCC_FILE_HAS_LINE" in
        yes)
          info "the cap IS in $SCC_ENV_FILE and a fresh session still does not see it — this is a PAM condition, NOT a missing line"
          info "re-running with --yes / --fix-sccache-cap will NOT help: either finds the line already present and changes nothing"
          info "check the session stack reads it:  grep -n pam_env /etc/pam.d/sudo /etc/pam.d/login /etc/pam.d/sshd   (each needs 'pam_env.so readenv=1')"
          ;;
        absent-file)
          if [ "$SCC_PLATFORM_UNMANAGED" = 1 ]; then
            info "this $PLATFORM host has no $SCC_ENV_FILE, so bootstrap has nowhere to persist it — set SCCACHE_CACHE_SIZE in this host's own session-startup mechanism (launchd/systemd/the image), NOT in a shell profile"
          else
            scc_fix_hint "this $PLATFORM host has no $SCC_ENV_FILE yet — the flag CREATES it carrying '$SCC_ENV_LINE', and pam_env reads it at session creation; --yes does it too"
          fi
          ;;
        *)
          [ -n "$SCC_PERSIST_NOTE" ] && info "nothing was persisted this run: $SCC_PERSIST_NOTE"
          scc_fix_hint "appends '$SCC_ENV_LINE' to $SCC_ENV_FILE; --yes does it too"
          ;;
      esac
      info "the gate reports the same fact on its accelerators line as sccache-cap=<bytes>(default) instead of (pinned)"
    fi
  fi
  # The oracle's private directories are removed here as well as in the trap: a bootstrap run is
  # long, and leaving them until process exit serves nothing.
  scc_cleanup
fi

# ---- 5c. Notification channel (ntfy) — issue #3119 ----
# The gate's push signal (#2667) and the worker supervisor page over ntfy. That
# dependency used to be an out-of-band, per-machine `/usr/local/bin/agent-notify`
# that this script never mentioned — and whose upstream v1.1.0 silently swallows
# the flag both callers passed, publishing a FAIL as a green priority-3 success.
# The payload contract now lives IN THE REPO (scripts/lib/gate-notify.sh), so the
# only pinned notify artifact travels in git. What a machine still needs is the
# transport (curl + python3) and a target; what bootstrap verifies is the
# CAPABILITY — the wrapper's own self-test publishes a PASS *and* a FAIL payload
# through a capture shim and validates both — never merely that a file exists
# (same posture as the mold link probe above).
hdr "Notification channel (ntfy, issue #3119)"
NOTIFY_LIB="$REPO_ROOT/scripts/lib/gate-notify.sh"
if [ ! -r "$NOTIFY_LIB" ]; then
  warn "scripts/lib/gate-notify.sh missing from this checkout — gate/worker notifications are no-ops"
else
  info "pinned: $(bash "$NOTIFY_LIB" --version 2>/dev/null || echo 'gate-notify contract v?')  (in-repo, git-pinned)"
  if have curl; then
    ok "curl present — ntfy publish transport"
  else
    warn "curl MISSING — notifications cannot be published"
    info "install curl via your package manager (apt/dnf/pacman/brew)"
  fi
  if have python3; then
    ok "python3 present — payload encoder"
  else
    warn "python3 MISSING — the notify payload cannot be encoded (notifications become no-ops)"
  fi
  NOTIFY_TARGET="${CQLITE_NOTIFY_WEBHOOK:-${CODEX_NOTIFY_WEBHOOK:-}}"
  if [ -n "$NOTIFY_TARGET" ]; then
    # Report the HOST only. TWO credential shapes must be stripped before anything
    # is echoed, because both reach a terminal or a CI log:
    #   userinfo      https://user:token@host/topic
    #   query/fragment  https://host?token=secret   (ntfy accepts a bare root, so
    #                   there is no '/' to bound the authority and the whole
    #                   "host?token=secret" would otherwise be printed)
    # Order matters: drop the query and fragment FIRST, then bound the authority,
    # then drop userinfo.
    NOTIFY_SCHEME="${NOTIFY_TARGET%%://*}"
    NOTIFY_REST="${NOTIFY_TARGET#*://}"
    NOTIFY_REST="${NOTIFY_REST%%\?*}"
    NOTIFY_REST="${NOTIFY_REST%%#*}"
    NOTIFY_HOSTPART="${NOTIFY_REST%%/*}"
    case "$NOTIFY_HOSTPART" in *@*) NOTIFY_HOSTPART="${NOTIFY_HOSTPART##*@}" ;; esac
    if [ "$NOTIFY_SCHEME" = "$NOTIFY_TARGET" ]; then
      ok "notify target configured (host not parseable from the value; not echoed)"
    else
      ok "notify target configured ($NOTIFY_SCHEME://$NOTIFY_HOSTPART/…)"
    fi
  else
    warn "no notify target configured — gate/worker notifications are silent no-ops on this machine"
    info "fleet mechanism is /etc/environment; add (and re-login):"
    info "  CODEX_NOTIFY_WEBHOOK=https://ntfy.sh/<your-topic>"
  fi
  # The CAPABILITY assert. Runs against a private curl capture shim inside the
  # wrapper's own tmpdir: no network, no real topic, nothing published anywhere.
  if have python3; then
    if selftest_out=$(bash "$NOTIFY_LIB" --self-test 2>&1); then
      ok "notify capability verified — $selftest_out"
    else
      warn "notify self-test FAILED — PASS/FAIL payloads do not satisfy the contract"
      printf '%s\n' "$selftest_out" | sed 's/^/         /'
    fi
  else
    warn "notify self-test skipped (needs python3) — capability UNVERIFIED on this machine"
  fi
  # Optional local desktop/sound adjunct. No version requirement: it is never in
  # the payload chain, and a hand-patched copy is never required by anything here.
  if have agent-notify; then
    # BOUNDED + stdin CLOSED, like every other call into this third-party binary:
    # measured in #3119, the pristine upstream copy HANGS when it inherits a tty
    # stdin, and bootstrap must never wedge on an optional version probe.
    NOTIFY_ADJUNCT_VER=""
    if have timeout && timeout --kill-after=1 1 true >/dev/null 2>&1; then
      NOTIFY_ADJUNCT_VER=$(timeout --kill-after=1 5 agent-notify --version 2>/dev/null </dev/null | head -1)
    fi
    info "optional local adjunct: ${NOTIFY_ADJUNCT_VER:-agent-notify (version not probed)} — desktop/sound only, no version requirement"
  else
    info "optional local adjunct agent-notify not installed — not needed; ntfy delivery is unaffected"
  fi
fi

# ---- 6. Health check: gate fmt + authoritative accelerators line ----
hdr "Health check (gate fmt + accelerators line)"
if [ "$SKIP_SMOKE" = 1 ]; then
  info "skipped (--skip-smoke)"
else
  smoke_out=$(bash "$GATE" --lite --only fmt 2>/dev/null) || smoke_out=$(bash "$GATE" --only fmt 2>&1)
  accel_line=$(printf '%s\n' "$smoke_out" | grep -E '^accelerators: ' | head -1)
  result_line=$(printf '%s\n' "$smoke_out" | grep -E '^RESULT: ' | head -1)
  if [ -n "$accel_line" ]; then
    ok "gate reports -> $accel_line"
  else
    warn "gate did not emit an accelerators line (is the gate healthy?)"
  fi
  [ -n "$result_line" ] && info "gate fmt smoke -> $result_line"
fi

# ---- Summary ----
hdr "Bootstrap summary"
if [ "$WARNINGS" -eq 0 ]; then
  printf '  \033[32mAll checks green.\033[0m This machine is ready for CQLite agent work.\n'
else
  printf '  \033[33m%d warning(s).\033[0m Address the [warn] lines above (or re-run with --yes to auto-install).\n' "$WARNINGS"
fi
info "Full doctrine: docs/development/agent-machine-setup.md"
# Informational bootstrap: DEFAULT is always exit 0 so it composes into setup scripts.
# That contract is preserved deliberately (issue #3369) — callers rely on it.
#
# --strict is the opt-in fail-closed signal for a machine PREFLIGHT (the agent-ami
# profile's verify.run uses it). Before it existed, the ONLY fail-closed signal was
# the presence of the literal string "All checks green." in stdout, so any caller that
# forgot to string-match — or matched it loosely — onboarded a broken box while this
# script exited 0. An exit code cannot be forgotten by accident.
if [ "$STRICT" = 1 ] && [ "$WARNINGS" -ne 0 ]; then
  info "--strict: exiting 1 ($WARNINGS warning(s)) — this machine is NOT certified ready"
  exit 1
fi
exit 0
