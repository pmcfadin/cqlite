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
#      the cross-machine lock does not work. Under --yes this configures a
#      credential path scoped to the origin host, preferring `gh auth setup-git`,
#      else a helper that dereferences $GH_TOKEN at call time. The token is never
#      written to disk.
#   4. roborev installed and its LOCAL config resolves — roborev follows THIS
#      machine's configured agent (commonly codex via .roborev.toml); we warn
#      only if the local config is broken, never prescribe an agent.
#   5. Datasets present + CQLITE_DATASETS_ROOT guidance.
#   6. Health check: run the gate's fmt component and print its authoritative
#      `accelerators:` line.
#
# Usage:
#   bash scripts/bootstrap-agent-machine.sh            # check + print install cmds
#   bash scripts/bootstrap-agent-machine.sh --yes      # also auto-run installs + git credentials
#   bash scripts/bootstrap-agent-machine.sh --skip-smoke   # skip the final gate run
#   bash scripts/bootstrap-agent-machine.sh --help
set -uo pipefail

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
GATE="$REPO_ROOT/scripts/agent-gate.sh"

AUTO_YES=0
SKIP_SMOKE=0
for arg in "$@"; do
  case "$arg" in
    --yes|-y) AUTO_YES=1 ;;
    --skip-smoke) SKIP_SMOKE=1 ;;
    -h|--help)
      # Print the whole leading comment block, bounded by the FIRST `set -` line
      # rather than a hardcoded line number: a fixed `2,45p` silently truncates the
      # help the moment the header grows (it already omitted the perf section).
      awk 'NR == 1 { next } /^set -/ { exit } { sub(/^# ?/, ""); print }' "${BASH_SOURCE[0]}"
      exit 0 ;;
    *) echo "bootstrap: unknown arg: $arg (try --help)" >&2; exit 2 ;;
  esac
done

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
TIMEOUT_BIN="$(command -v timeout 2>/dev/null || command -v gtimeout 2>/dev/null || true)"
# bounded <secs> <cmd...> — run <cmd...> under the resolved timeout binary if there is
# one, else run it directly. Use `env VAR=... cmd` when the call needs env prefixes.
bounded() {
  local secs="$1"; shift
  if [ -n "$TIMEOUT_BIN" ]; then "$TIMEOUT_BIN" "$secs" "$@"; else "$@"; fi
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
  PERF_DROPIN=$(perf_capability_dropin_path)

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
    if [ "$PERF_PRIV_STATE" = no-sudo-binary ]; then
      info "no 'sudo' on this box — write + apply from a ROOT shell:  bash scripts/perf-capability.sh --drop-in > $PERF_DROPIN && sysctl -q --system"
      info "(or ask the image/host owner to install it; without the drop-in this box reverts to perf_event_paranoid=4 on reboot)"
    else
      info "write + apply the drop-in:  bash scripts/perf-capability.sh --drop-in | ${PERF_RUN_AS}tee $PERF_DROPIN >/dev/null && ${PERF_RUN_AS}sysctl -q --system"
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
      if perf_capability_dropin_install ${PERF_ROOT[@]+"${PERF_ROOT[@]}"}; then
        ok "wrote $PERF_DROPIN (kernel.perf_event_paranoid = -1, kernel.kptr_restrict = 0)"
        PERF_DROPIN_OK=1
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
git_origin_host() {
  local url="$1" rest hostport
  case "$url" in
    https://*) rest="${url#https://}" ;;
    http://*)  rest="${url#http://}" ;;
    *) return 0 ;;
  esac
  hostport="${rest%%/*}"
  printf '%s' "${hostport#*@}"
}

hdr "git push credentials (issue #2942)"
# Classify origin BEFORE probing: only an http(s) remote uses a credential helper.
# An SSH remote authenticates with a key (a helper is irrelevant), and a local/file
# remote needs no credential at all — mislabeling either would send an operator
# after the wrong fix, which is the exact failure mode this whole change exists to end.
GIT_ORIGIN_URL=""; GIT_ORIGIN_HOST=""; GIT_ORIGIN_KIND=none
if have git; then
  GIT_ORIGIN_URL=$(git -C "$REPO_ROOT" remote get-url origin 2>/dev/null || true)
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
  warn "no 'origin' remote in $REPO_ROOT — cannot check push credentials"
elif [ "$GIT_ORIGIN_KIND" = ssh ]; then
  # SSH (git@host:… / ssh://…): git authenticates with your SSH key, and an https
  # credential helper is irrelevant. Report it and configure nothing.
  ok "origin is an SSH remote — git push authenticates via SSH keys, not a credential helper (no helper needed)"
  info "verify separately if pushes fail:  ssh -T git@github.com"
elif [ "$GIT_ORIGIN_KIND" = other ]; then
  info "origin is a '$GIT_ORIGIN_KIND' remote (neither http(s) nor SSH) — no credential helper applies"
elif git_cred_probe "$GIT_ORIGIN_HOST"; then
  ok "git push credentials resolve for $GIT_ORIGIN_HOST (a helper answers with a non-empty secret)"
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
  warn "git push has NO credentials for $GIT_ORIGIN_HOST — an authenticated 'gh' does NOT authenticate git"
  info "symptom: every push fails with  fatal: could not read Username for 'https://$GIT_ORIGIN_HOST'"
  info "impact: scripts/flow/claim.sh + claim-heartbeat.sh push on 10+ call sites — the claim protocol does not work"
  info "fix:    gh auth setup-git    (preferred; wires gh as git's credential helper)"
  info "        or re-run with --yes to configure a helper that reads \$GH_TOKEN at call time"
  if [ "$AUTO_YES" = 1 ]; then
    cred_fixed=0
    # Preferred form: let gh wire itself in. Verified by RE-PROBING — on the box
    # that motivated #2942 the gh credential path was precisely what was not wired,
    # so "the command exited 0" is not evidence.
    if have gh && gh auth status >/dev/null 2>&1; then
      info "configuring: gh auth setup-git"
      if gh auth setup-git >/dev/null 2>&1 && git_cred_probe "$GIT_ORIGIN_HOST"; then
        ok "git credentials configured via 'gh auth setup-git' (no secret written to disk)"
        cred_fixed=1
      else
        info "'gh auth setup-git' did not yield a usable credential — falling back to the \$GH_TOKEN helper"
      fi
    fi
    if [ "$cred_fixed" = 0 ]; then
      if [ -z "${GH_TOKEN:-${GITHUB_TOKEN:-}}" ]; then
        warn "cannot auto-configure: neither GH_TOKEN nor GITHUB_TOKEN is set in this environment"
        info "export GH_TOKEN=<token>, then re-run:  bash scripts/bootstrap-agent-machine.sh --yes"
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
          warn "a \$GH_TOKEN-style helper is ALREADY configured for $GIT_ORIGIN_HOST yet the probe still fails — check that GH_TOKEN is set, valid and unexpired (not re-adding it)"
        elif git config --global --add "$cred_key" "$GIT_CRED_HELPER" 2>/dev/null \
           && git_cred_probe "$GIT_ORIGIN_HOST"; then
          ok "git credentials configured for $GIT_ORIGIN_HOST via a \$GH_TOKEN-dereferencing helper (no secret written to disk)"
          info "this helper reads \$GH_TOKEN from the ENVIRONMENT — an unattended worker (systemd/cron)"
          info "started without GH_TOKEN exported will still fail every push; prefer 'gh auth setup-git' there"
        else
          warn "could not configure a working git credential helper — fix manually: gh auth setup-git"
        fi
      fi
    fi
  else
    info "(re-run with --yes to auto-configure)"
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

# ---- 5b. Single-gate default: one full gate per box (issue #2640) ----
# One worker per machine (#1930) runs its full gates serially, so the DEFAULT
# posture is a SINGLE full gate at a time. Pin CQLITE_GATE_MAX_CONCURRENCY=1 in the
# shell profile so (a) the #1825 machine-wide cap admits exactly one full gate and
# (b) the #2640 per-gate core budget hands that sole gate the FULL core count —
# no CPU oversubscription, no manual pgrep-serialization. A machine that
# deliberately wants >1 concurrent gate overrides the export.
hdr "Single-gate default (CQLITE_GATE_MAX_CONCURRENCY=1, issue #2640)"
PROFILE=""
case "${SHELL:-}" in
  */zsh) PROFILE="$HOME/.zshrc" ;;
  */bash) PROFILE="$HOME/.bashrc" ;;
  *) PROFILE="${ENV:-$HOME/.profile}" ;;
esac
EXPORT_LINE='export CQLITE_GATE_MAX_CONCURRENCY=1  # cqlite: one full gate per box, full cores (issue #2640)'
if [ -n "${CQLITE_GATE_MAX_CONCURRENCY:-}" ]; then
  ok "CQLITE_GATE_MAX_CONCURRENCY already set to '${CQLITE_GATE_MAX_CONCURRENCY}' in this environment"
elif [ -n "$PROFILE" ] && [ -f "$PROFILE" ] && grep -q 'CQLITE_GATE_MAX_CONCURRENCY' "$PROFILE" 2>/dev/null; then
  ok "CQLITE_GATE_MAX_CONCURRENCY already pinned in $PROFILE"
elif [ "$AUTO_YES" = 1 ] && [ -n "$PROFILE" ]; then
  printf '%s\n' "$EXPORT_LINE" >>"$PROFILE" \
    && ok "pinned CQLITE_GATE_MAX_CONCURRENCY=1 in $PROFILE (re-source or open a new shell)" \
    || warn "could not append to $PROFILE — add manually: $EXPORT_LINE"
else
  warn "CQLITE_GATE_MAX_CONCURRENCY not pinned — one full gate per box is the default posture (#2640)"
  info "add to ${PROFILE:-your shell profile}:  $EXPORT_LINE"
  info "(re-run with --yes to auto-append)"
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
# Informational bootstrap: always exit 0 so it composes into setup scripts.
exit 0
