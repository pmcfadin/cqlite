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
#   3. gh auth + the `project` scope (Path A board dispatch, #1886).
#   4. roborev installed and its LOCAL config resolves — roborev follows THIS
#      machine's configured agent (commonly codex via .roborev.toml); we warn
#      only if the local config is broken, never prescribe an agent.
#   5. Datasets present + CQLITE_DATASETS_ROOT guidance.
#   6. Health check: run the gate's fmt component and print its authoritative
#      `accelerators:` line.
#
# Usage:
#   bash scripts/bootstrap-agent-machine.sh            # check + print install cmds
#   bash scripts/bootstrap-agent-machine.sh --yes      # also auto-run brew/cargo installs
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
      sed -n '2,31p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
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

# ---- 3. gh auth + project scope (Path A, #1886) ----
hdr "GitHub CLI auth + project scope (Path A, #1886)"
if have gh; then
  if gh auth status >/dev/null 2>&1; then
    ok "gh authenticated"
    # Token-boundary match on the scopes line so a scope like 'project:read-only'
    # (or any 'xprojecty' substring) can never false-positive as 'project'.
    scopes_line=$(gh auth status 2>&1 | grep "Token scopes:" | head -1)
    if printf '%s\n' "$scopes_line" | grep -qE "(^|[ ,'])project([ ,']|$)"; then
      ok "'project' scope present — board dispatch works"
    else
      warn "'project' scope MISSING — the board is the SOLE dispatch authority (Path A). Fix:"
      info "gh auth refresh -s project"
    fi
  else
    warn "gh not authenticated — run: gh auth login (then gh auth refresh -s project)"
  fi
else
  warn "gh CLI NOT installed — $(brew_or_cargo gh gh 2>/dev/null || echo 'install GitHub CLI: https://cli.github.com')"
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
