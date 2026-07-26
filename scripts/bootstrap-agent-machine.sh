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
#   3. gh auth + BOARD ACCESS (Path A board dispatch, #1886). The verdict comes
#      from a READ-ONLY functional probe of the board, never from the `project`
#      scope string (issue #2942): a token can carry `project` and still fail
#      `gh project item-edit` for a missing `read:org` while the equivalent
#      `updateProjectV2ItemFieldValue` GraphQL mutation works. Overridable with
#      CQLITE_PROJECT_OWNER / CQLITE_PROJECT_NUMBER.
#   3b. git push CREDENTIALS (issue #2942) — separate from `gh` auth. The claim
#      protocol (scripts/flow/claim.sh, claim-heartbeat.sh) pushes with plain git
#      on 10+ call sites, so an authenticated gh with an unauthenticated git means
#      the cross-machine lock does not work. Under --yes this configures a
#      credential path, preferring `gh auth setup-git`, else a helper that
#      dereferences $GH_TOKEN at call time. The token is never written to disk.
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
      sed -n '2,42p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
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

# ---- 3. GitHub board access + project scope (Path A, #1886; #2942) ----
# The board is the SOLE dispatch authority, so "can this machine use the board?"
# must be answered by TRYING it. Until #2942 this section matched the `project`
# scope STRING and declared "board dispatch works" — and a box was observed where
# that scope IS present, `gh project item-edit` fails for a missing `read:org`, and
# the equivalent `updateProjectV2ItemFieldValue` GraphQL mutation succeeds with the
# SAME token. A scope match is evidence about a token, not about the operation.
# The scope check survives as a cheap PRE-FILTER; the verdict comes from a probe.
# The probe is strictly READ-ONLY — a bootstrap must never mutate a real board item.
BOARD_OWNER="${CQLITE_PROJECT_OWNER:-pmcfadin}"
BOARD_NUMBER="${CQLITE_PROJECT_NUMBER:-1}"
BOARD_GRAPHQL_WRITE="updateProjectV2ItemFieldValue"

# board_graphql_read <owner> <number> — READ-ONLY projectV2 lookup, the read
# counterpart of the write fallback, so its result says whether that fallback is
# actually available on this token. Success requires a NON-EMPTY project id:
# `gh api graphql` exits 0 on a well-formed query that resolves to null (e.g. an
# org-owned board queried as a user), which would otherwise read as a false OK.
# Tries user-owned first (what project-board-sync.yml uses), then org-owned.
board_graphql_read() {
  local owner="$1" number="$2" id
  id=$(gh api graphql -f owner="$owner" -F number="$number" \
        -f query='query($owner:String!,$number:Int!){user(login:$owner){projectV2(number:$number){id}}}' \
        --jq '.data.user.projectV2.id' 2>/dev/null)
  if [ -z "$id" ] || [ "$id" = null ]; then
    id=$(gh api graphql -f owner="$owner" -F number="$number" \
          -f query='query($owner:String!,$number:Int!){organization(login:$owner){projectV2(number:$number){id}}}' \
          --jq '.data.organization.projectV2.id' 2>/dev/null)
  fi
  [ -n "$id" ] && [ "$id" != null ]
}

hdr "GitHub board access + project scope (Path A, #1886)"
if have gh; then
  # ONE invocation: capture output and status together (gh auth status hits the API).
  auth_out=$(gh auth status 2>&1); auth_rc=$?
  if [ "$auth_rc" -eq 0 ]; then
    ok "gh authenticated"
    # Token-boundary match on the scopes line so a scope like 'project:read-only'
    # (or any 'xprojecty' substring) can never false-positive as 'project'.
    scopes_line=$(printf '%s\n' "$auth_out" | grep "Token scopes:" | head -1)
    scope_prefilter=0
    if printf '%s\n' "$scopes_line" | grep -qE "(^|[ ,'])project([ ,']|$)"; then
      scope_prefilter=1
      info "pre-filter: 'project' scope present (NOT the verdict — the probe below decides)"
    else
      warn "'project' scope MISSING — the board is the SOLE dispatch authority (Path A). Fix:"
      info "gh auth refresh -s project"
    fi
    # gh's OWN declaration that the ACTIVE token lacks scopes gh requires. This is
    # the discriminator behind the observed false OK: scopes include 'project',
    # `read:org` is missing, and `gh project item-edit` fails on that alone.
    missing_scopes=$(printf '%s\n' "$auth_out" | grep -i "Missing required token scopes" | head -1)
    missing_list="${missing_scopes##*scopes:}"; missing_list="${missing_list# }"
    # READ-ONLY probe 1: can `gh project` see the board at all?
    board_read=1
    gh project view "$BOARD_NUMBER" --owner "$BOARD_OWNER" >/dev/null 2>&1 || board_read=0
    # READ-ONLY probe 2: does the GraphQL projectV2 surface answer with this token?
    graphql_read=1
    board_graphql_read "$BOARD_OWNER" "$BOARD_NUMBER" || graphql_read=0
    if [ "$board_read" = 1 ] && [ -z "$missing_scopes" ]; then
      ok "board #$BOARD_NUMBER ($BOARD_OWNER) reachable — 'gh project' read probe OK, no missing token scopes"
    elif [ "$board_read" = 1 ]; then
      warn "board READ works but gh reports the active token is MISSING required scopes ($missing_list) — 'gh project item-edit' can still FAIL for read:org"
      info "board WRITES: fall back to the GraphQL \`$BOARD_GRAPHQL_WRITE\` mutation (it succeeds with the SAME token; graphql projectV2 read probe: $([ "$graphql_read" = 1 ] && echo OK || echo FAILED))"
      info "or widen the token:  gh auth refresh -s read:org"
    elif [ "$graphql_read" = 1 ]; then
      warn "'gh project' CANNOT use board #$BOARD_NUMBER ($BOARD_OWNER)$([ "$scope_prefilter" = 1 ] && printf ' even though the scope pre-filter passed')${missing_list:+ — gh reports missing required scopes ($missing_list)}"
      info "the GraphQL projectV2 read probe SUCCEEDED with the same token — board WRITES must go through the \`$BOARD_GRAPHQL_WRITE\` mutation, not \`gh project item-edit\`"
      info "or widen the token:  gh auth refresh -s read:org"
    else
      warn "board #$BOARD_NUMBER ($BOARD_OWNER) UNREACHABLE — BOTH probes failed ('gh project view' and the GraphQL projectV2 read). Path A: a session with no board access must STOP, never label-dispatch"
      info "check the owner/number (CQLITE_PROJECT_OWNER / CQLITE_PROJECT_NUMBER), then: gh auth refresh -s project -s read:org"
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
  local host="$1" out to=""
  command -v timeout >/dev/null 2>&1 && to="timeout 10"
  # shellcheck disable=SC2086  # $to is an intentional (possibly empty) command prefix
  out=$(printf 'protocol=https\nhost=%s\n\n' "$host" \
    | GIT_TERMINAL_PROMPT=0 GIT_ASKPASS=cqlite-bootstrap-no-askpass \
      SSH_ASKPASS=cqlite-bootstrap-no-askpass \
      $to git -C "$REPO_ROOT" credential fill 2>/dev/null) || true
  printf '%s\n' "$out" | grep -q '^password=.'
}

# git_global_helper_configured — 0 iff ANY global credential helper exists, scoped
# (credential.https://host.helper) or not (credential.helper).
git_global_helper_configured() {
  git config --global --get-regexp '^credential\..*helper$' >/dev/null 2>&1
}

# git_env_token_helper_active <host> — 0 iff the helper answering for <host> is OUR
# $GH_TOKEN-dereferencing one, at any scope. Used to attach the env-var dependency
# caveat to an otherwise-green verdict.
git_env_token_helper_active() {
  { git config --global --get-all "credential.https://$1.helper" 2>/dev/null
    git config --global --get-all credential.helper 2>/dev/null
    git -C "$REPO_ROOT" config --get-all credential.helper 2>/dev/null
  } | grep -qF 'x-access-token'
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
  if git -C "$REPO_ROOT" config --local --get-all credential.helper >/dev/null 2>&1 \
     && ! git_global_helper_configured; then
    info "note: the helper is configured at REPO-LOCAL scope only — a fresh clone or a"
    info "      new checkout on this box will NOT inherit it. Re-run with --yes to add a global one."
  fi
  if git_env_token_helper_active "$GIT_ORIGIN_HOST"; then
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
