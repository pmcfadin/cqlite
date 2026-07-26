#!/usr/bin/env bash
# Regression test for scripts/bootstrap-agent-machine.sh (issue #1921).
#
# Covers the PURE-CHECK paths only: the bootstrap must, in its default mode
# (no --yes), run all its checks and NEVER install anything — it may only print
# install commands. Fast by design: runs with --skip-smoke so it never invokes
# the multi-minute gate.
#
# Run standalone:   bash scripts/tests/test_bootstrap_agent_machine.sh
# Or via the gate:  scripts/agent-gate.sh runs it in the `tooling-tests` component.
set -uo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
BOOTSTRAP="$SCRIPT_DIR/../bootstrap-agent-machine.sh"

PASS=0
FAIL=0
ok()  { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad() { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }

# --- 1. syntax check (bash -n) ---
if bash -n "$BOOTSTRAP" 2>/dev/null; then
  ok "bootstrap script parses (bash -n)"
else
  bad "bootstrap script has a syntax error"
fi

# --- 2. --help exits 0 and prints usage ---
help_out=$(bash "$BOOTSTRAP" --help 2>&1); help_rc=$?
if [ "$help_rc" -eq 0 ] && printf '%s' "$help_out" | grep -q "bootstrap"; then
  ok "--help exits 0 and prints usage"
else
  bad "--help did not exit 0 / print usage (rc=$help_rc)"
fi

# --- 3. Pure-check run must NOT install. Shadow brew/cargo/roborev with a tripwire
#        on PATH so ANY install attempt is recorded, then assert nothing ran an
#        install subcommand. ---
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bootstrap-test.XXXXXX")
trap 'rm -rf "$tmp"' EXIT
tripwire="$tmp/tripwire.log"
: >"$tripwire"

# --- GLOBAL git-config isolation (issue #2942) -----------------------------
# The bootstrap's git-credential section READS the configured helper chain and,
# under --yes, WRITES a helper into the user's GLOBAL git config. This self-test
# must never touch (or be perturbed by) the host machine's real credential setup —
# clobbering it would break the live delivery session running on this box. These
# two exports are inherited by EVERY bootstrap child below, so the isolation holds
# for cases added later without remembering to opt in:
#   GIT_CONFIG_GLOBAL   redirects `git config --global` + global reads to a throwaway
#   GIT_CONFIG_NOSYSTEM ignores /etc/gitconfig, so a host-wide helper cannot leak in
# (HOME is sandboxed per case as well; GIT_CONFIG_GLOBAL is the belt to that braces —
# it also covers an XDG_CONFIG_HOME that survives a HOME override.)
export GIT_CONFIG_GLOBAL="$tmp/global-gitconfig"
export GIT_CONFIG_NOSYSTEM=1
: >"$GIT_CONFIG_GLOBAL"

# --- BOARD env isolation (issue #2942) -------------------------------------
# The board section reads CQLITE_PROJECT_{OWNER,NUMBER,ACCOUNT} and PROJECT_TITLE from
# the environment, and a worker shell commonly EXPORTS them (the fleet exports
# CQLITE_PROJECT_NUMBER). Inheriting them makes this suite's verdict depend on the shell
# it runs in — it silently masked the entire "number not exported" path until a case was
# written for it. Clear them once; every case sets exactly what it means to test.
unset CQLITE_PROJECT_NUMBER CQLITE_PROJECT_OWNER CQLITE_PROJECT_ACCOUNT PROJECT_TITLE

mkshim() {
  # mkshim <name>: a fake tool that records "install"/"add" invocations and is
  # otherwise a harmless no-op (version/status queries succeed emptily).
  local name="$1"
  cat >"$tmp/$name" <<EOF
#!/usr/bin/env bash
for a in "\$@"; do
  case "\$a" in
    install|add) echo "$name \$*" >>"$tripwire" ;;
  esac
done
exit 0
EOF
  chmod +x "$tmp/$name"
}
mkshim brew
mkshim cargo
mkshim roborev
mkshim gh

# Sandbox HOME/CARGO_HOME for these whole-script runs so the Linux mold branch can
# NEVER mutate the host's real ~/.cargo/config when this runs inside tooling-tests
# on a Linux gate box that has mold + a working cc (issue #2859 blocker 2).
host_home="$tmp/host-home"; mkdir -p "$host_home/.cargo"

# Run with the shims FIRST on PATH, default mode (no --yes), skipping the smoke.
run_out=$(PATH="$tmp:$PATH" HOME="$host_home" CARGO_HOME="$host_home/.cargo" \
  bash "$BOOTSTRAP" --skip-smoke 2>&1); run_rc=$?

if [ "$run_rc" -eq 0 ]; then
  ok "default (no --yes) run exits 0"
else
  bad "default run exited non-zero (rc=$run_rc)"
  printf '%s\n' "$run_out"
fi

if [ -s "$tripwire" ]; then
  bad "default run attempted an install (tripwire):"
  cat "$tripwire"
else
  ok "default run performed NO installs (pure check)"
fi

# --- 4. The run must actually emit its section headers (it ran the checks). ---
for section in "Rust toolchain" "Gate accelerators" "project scope" "roborev" "CQLITE_DATASETS_ROOT" "Bootstrap summary"; do
  if printf '%s' "$run_out" | grep -q "$section"; then
    ok "check section present: $section"
  else
    bad "check section MISSING: $section"
  fi
done

# --- 5. Default run must PRINT an install command for a missing tool rather than
#        run it. Force a missing accelerator by running with an empty-ish PATH that
#        still has coreutils but no sccache; assert the guidance line appears. ---
# Reset the tripwire so the no-install assertion below reflects ONLY this run.
: >"$tripwire"
guard_out=$(PATH="$tmp:/usr/bin:/bin" HOME="$host_home" CARGO_HOME="$host_home/.cargo" \
  bash "$BOOTSTRAP" --skip-smoke 2>&1)
if printf '%s' "$guard_out" | grep -Eq "install sccache:|sccache MISSING"; then
  ok "missing accelerator prints install guidance (does not auto-install)"
else
  bad "missing accelerator did not surface install guidance"
fi
if [ -s "$tripwire" ]; then
  bad "guidance run STILL attempted an install"
else
  ok "guidance run performed NO installs"
fi

# --- 6. mold link accelerator on Linux (issue #2859) ------------------------
# All cases below stub `uname` (to simulate the OS), `mold`, and the C compilers,
# and point HOME/CARGO_HOME at a sandbox so the managed block is written to a
# throwaway ~/.cargo/config.toml — never the real one and never the repo's.

mk_stub() {
  # mk_stub <dir> <name> <body>
  local dir="$1" name="$2" body="$3"
  cat >"$dir/$name" <<EOF
#!/usr/bin/env bash
$body
EOF
  chmod +x "$dir/$name"
}
# count_begin <file>: number of managed-block BEGIN markers. grep -c already prints
# a count (0 on no match) AND exits 1 — a `|| echo 0` would DOUBLE-print "0\n0", so
# capture the count and default an empty (missing-file) result to 0 instead.
count_begin() {
  local n
  n=$(grep -c '^# BEGIN cqlite-mold' "$1" 2>/dev/null)
  echo "${n:-0}"
}
# Stub gh + roborev + cargo so the (unrelated) auth/agent/toolchain sections stay
# fast and offline during these mold cases, which run bootstrap under the full PATH
# with CARGO_HOME pointed at a throwaway dir (a real `cargo --version` there would
# trigger a multi-minute rustup toolchain provision into the empty CARGO_HOME).
stub_net() {
  mk_stub "$1" gh 'exit 0'
  mk_stub "$1" roborev 'exit 0'
  mk_stub "$1" cargo '[ "$1" = --version ] && echo "cargo 1.88.0"; exit 0'
}

# mk_hermetic_bin <dir>: a stub-only PATH dir with symlinked coreutils + a Linux
# `uname` stub, so the missing-mold cases (6g/6h) never depend on the host having (or
# NOT having) apt-get/dnf/etc. On a real Linux runner `/usr/bin/apt-get` exists, which
# would otherwise flip the "no supported package manager" assertion and turn the FULL
# gate RED via tooling-tests (#2859 blocker D). No package-manager binaries are linked;
# callers add exactly the ones they intend to detect.
mk_hermetic_bin() {
  local dir="$1" t p
  mkdir -p "$dir"
  for t in bash dirname mktemp grep cp cat sed awk mkdir rm ln mv touch chmod \
           head tail tr sort cut wc stat env git find xargs basename date sleep expr \
           timeout gtimeout; do   # BOTH: stock macOS has only gtimeout (GNU coreutils)
    p=$(type -P "$t" 2>/dev/null) || continue
    [ -n "$p" ] && ln -sf "$p" "$dir/$t" 2>/dev/null || true
  done
  mk_stub "$dir" uname 'echo Linux; exit 0'
  stub_net "$dir"  # gh/roborev/cargo stubs — no live network from these cases
}

# 6a. mold present + cc passes the probe -> managed block written, both Linux
#     triples, NO linker line (default cc accepts -fuse-ld=mold).
sbA=$(mktemp -d "$tmp/moldA.XXXXXX"); stubA="$tmp/stubA"; mkdir -p "$stubA"
mk_stub "$stubA" uname 'echo Linux; exit 0'
stub_net "$stubA"
mk_stub "$stubA" mold '[ "$1" = --version ] && echo "mold 2.4.0"; exit 0'
mk_stub "$stubA" cc 'exit 0'
outA=$(PATH="$stubA:$PATH" HOME="$sbA" CARGO_HOME="$sbA/.cargo" \
  bash "$BOOTSTRAP" --skip-smoke 2>&1)
cfgA="$sbA/.cargo/config.toml"
if printf '%s' "$outA" | grep -q "Link accelerator: mold"; then
  ok "mold: Linux run emits the mold section"
else
  bad "mold: Linux run did not emit the mold section"
fi
if [ -f "$cfgA" ] \
   && grep -q '^# BEGIN cqlite-mold' "$cfgA" \
   && grep -q '^# END cqlite-mold' "$cfgA" \
   && grep -q '^\[target.x86_64-unknown-linux-gnu\]' "$cfgA" \
   && grep -q '^\[target.aarch64-unknown-linux-gnu\]' "$cfgA" \
   && grep -q 'link-arg=-fuse-ld=mold' "$cfgA"; then
  ok "mold: managed block written with both Linux target triples"
else
  bad "mold: managed block missing expected markers/triples"
  [ -f "$cfgA" ] && { echo "--- config ---"; cat "$cfgA"; echo "--------------"; }
fi
if [ -f "$cfgA" ] && ! grep -q '^linker = ' "$cfgA"; then
  ok "mold: cc-passing probe writes NO linker override"
else
  bad "mold: cc-passing probe unexpectedly wrote a linker override"
fi

# 6b. re-run is byte-idempotent: block appears exactly once and the file is
#     identical to the first run.
firstA=$(cat "$cfgA")
PATH="$stubA:$PATH" HOME="$sbA" CARGO_HOME="$sbA/.cargo" \
  bash "$BOOTSTRAP" --skip-smoke >/dev/null 2>&1
secondA=$(cat "$cfgA")
if [ "$(count_begin "$cfgA")" = 1 ] && [ "$firstA" = "$secondA" ]; then
  ok "mold: re-run idempotent (exactly one block, file byte-identical)"
else
  bad "mold: re-run not idempotent (begin-count=$(count_begin "$cfgA"))"
fi

# 6c. unrelated user config outside the markers is preserved byte-for-byte.
sbC=$(mktemp -d "$tmp/moldC.XXXXXX"); mkdir -p "$sbC/.cargo"
cfgC="$sbC/.cargo/config.toml"
printf '[build]\njobs = 7\n\n[net]\nretry = 9\n' >"$cfgC"
PATH="$stubA:$PATH" HOME="$sbC" CARGO_HOME="$sbC/.cargo" \
  bash "$BOOTSTRAP" --skip-smoke >/dev/null 2>&1
if grep -qx 'jobs = 7' "$cfgC" && grep -qx 'retry = 9' "$cfgC" \
   && grep -qx '\[build\]' "$cfgC" && grep -qx '\[net\]' "$cfgC" \
   && grep -q '^# BEGIN cqlite-mold' "$cfgC"; then
  ok "mold: unrelated user config preserved alongside the appended block"
else
  bad "mold: user config not preserved when appending the block"
  echo "--- config ---"; cat "$cfgC"; echo "--------------"
fi
# Idempotent even with user content present.
firstC=$(cat "$cfgC")
PATH="$stubA:$PATH" HOME="$sbC" CARGO_HOME="$sbC/.cargo" \
  bash "$BOOTSTRAP" --skip-smoke >/dev/null 2>&1
if [ "$firstC" = "$(cat "$cfgC")" ] && [ "$(count_begin "$cfgC")" = 1 ]; then
  ok "mold: re-run with user content stays byte-identical (one block)"
else
  bad "mold: re-run with user content changed the file or duplicated the block"
fi

# 6d. failed link probe (no compiler accepts -fuse-ld=mold) -> warn, write NOTHING.
sbD=$(mktemp -d "$tmp/moldD.XXXXXX"); stubD="$tmp/stubD"; mkdir -p "$stubD"
mk_stub "$stubD" uname 'echo Linux; exit 0'
stub_net "$stubD"
mk_stub "$stubD" mold 'exit 0'
mk_stub "$stubD" cc 'exit 1'
mk_stub "$stubD" clang 'exit 1'
outD=$(PATH="$stubD:$PATH" HOME="$sbD" CARGO_HOME="$sbD/.cargo" \
  bash "$BOOTSTRAP" --skip-smoke 2>&1)
if printf '%s' "$outD" | grep -q "link probe FAILED" \
   && [ ! -f "$sbD/.cargo/config.toml" ]; then
  ok "mold: failed link probe warns and writes no linker config"
else
  bad "mold: failed link probe still wrote config or missed the warning"
  [ -f "$sbD/.cargo/config.toml" ] && cat "$sbD/.cargo/config.toml"
fi

# 6e. clang-only variant: cc fails the probe, clang passes -> block sets linker.
sbE=$(mktemp -d "$tmp/moldE.XXXXXX"); stubE="$tmp/stubE"; mkdir -p "$stubE"
mk_stub "$stubE" uname 'echo Linux; exit 0'
stub_net "$stubE"
mk_stub "$stubE" mold 'exit 0'
mk_stub "$stubE" cc 'exit 1'
mk_stub "$stubE" clang 'exit 0'
PATH="$stubE:$PATH" HOME="$sbE" CARGO_HOME="$sbE/.cargo" \
  bash "$BOOTSTRAP" --skip-smoke >/dev/null 2>&1
cfgE="$sbE/.cargo/config.toml"
if [ -f "$cfgE" ] && [ "$(grep -c '^linker = "clang"' "$cfgE")" = 2 ]; then
  ok "mold: clang-only probe writes linker = \"clang\" for both triples"
else
  bad "mold: clang-only probe did not set linker for both triples"
  [ -f "$cfgE" ] && { echo "--- config ---"; cat "$cfgE"; echo "--------------"; }
fi

# 6f. Darwin no-op: mold section skipped, no config written.
sbF=$(mktemp -d "$tmp/moldF.XXXXXX"); stubF="$tmp/stubF"; mkdir -p "$stubF"
mk_stub "$stubF" uname 'echo Darwin; exit 0'
stub_net "$stubF"
mk_stub "$stubF" mold '[ "$1" = --version ] && echo "mold 2.4.0"; exit 0'
mk_stub "$stubF" cc 'exit 0'
outF=$(PATH="$stubF:$PATH" HOME="$sbF" CARGO_HOME="$sbF/.cargo" \
  bash "$BOOTSTRAP" --skip-smoke 2>&1)
if ! printf '%s' "$outF" | grep -q "Link accelerator: mold" \
   && [ ! -f "$sbF/.cargo/config.toml" ]; then
  ok "mold: Darwin performs no mold detection/config (no-op)"
else
  bad "mold: Darwin unexpectedly ran the mold section or wrote config"
fi

# 6g. missing mold + a supported package manager -> prints the install command in
#     default (no --yes) mode and installs NOTHING; writes no linker config. Runs in
#     a HERMETIC stub-only PATH (blocker D): the ONLY package manager visible is the
#     apt-get stub we add, regardless of what the host has installed.
sbG=$(mktemp -d "$tmp/moldG.XXXXXX"); stubG="$tmp/stubG"
mk_hermetic_bin "$stubG"
tripG="$stubG/tripwire.log"; : >"$tripG"
mk_stub "$stubG" apt-get "echo \"apt-get \$*\" >>\"$tripG\"; exit 0"
outG=$(PATH="$stubG" HOME="$sbG" CARGO_HOME="$sbG/.cargo" \
  bash "$BOOTSTRAP" --skip-smoke 2>&1)
if printf '%s' "$outG" | grep -q "mold MISSING" \
   && printf '%s' "$outG" | grep -q "install mold:.*apt-get install -y mold" \
   && [ ! -s "$tripG" ] \
   && [ ! -f "$sbG/.cargo/config.toml" ]; then
  ok "mold: missing + apt prints install command, installs nothing, writes no config"
else
  bad "mold: missing+apt path did not print-only (tripwire=$(cat "$tripG" 2>/dev/null))"
  printf '%s\n' "$outG" | grep -i mold
fi

# 6h. missing mold + NO supported package manager -> warn, no config. HERMETIC PATH
#     (blocker D) so no host apt-get/dnf/etc. is visible.
sbH=$(mktemp -d "$tmp/moldH.XXXXXX"); stubH="$tmp/stubH"
mk_hermetic_bin "$stubH"
outH=$(PATH="$stubH" HOME="$sbH" CARGO_HOME="$sbH/.cargo" \
  bash "$BOOTSTRAP" --skip-smoke 2>&1)
if printf '%s' "$outH" | grep -q "no supported package manager" \
   && [ ! -f "$sbH/.cargo/config.toml" ]; then
  ok "mold: missing + no package manager warns and writes no config"
else
  bad "mold: missing + no-manager path missed the warn or wrote config"
  printf '%s\n' "$outH" | grep -i mold
fi

# 6j. legacy extension-less ~/.cargo/config (blocker A): the block must be written
#     INTO the existing `config` cargo actually reads — NOT a shadow `config.toml`
#     that cargo would silently prefer, dropping the user's whole config.
sbJ=$(mktemp -d "$tmp/moldJ.XXXXXX"); mkdir -p "$sbJ/.cargo"
printf '[net]\nretry = 4\n' >"$sbJ/.cargo/config"
PATH="$stubA:$PATH" HOME="$sbJ" CARGO_HOME="$sbJ/.cargo" \
  bash "$BOOTSTRAP" --skip-smoke >/dev/null 2>&1
if grep -q '^# BEGIN cqlite-mold' "$sbJ/.cargo/config" \
   && grep -qx 'retry = 4' "$sbJ/.cargo/config" \
   && [ ! -f "$sbJ/.cargo/config.toml" ]; then
  ok "mold: writes into the legacy extension-less ~/.cargo/config (no shadow config.toml)"
else
  bad "mold: legacy config handling wrong (shadow config.toml or lost user config)"
  ls -la "$sbJ/.cargo" 2>/dev/null
fi

# 6k. pre-existing user [target.<triple>-unknown-linux-gnu] OUTSIDE the markers
#     (blocker B): appending our block would be a TOML table redefinition = cargo
#     parse error on every invocation. Bootstrap must WARN and write NOTHING, leaving
#     the file byte-identical.
sbK=$(mktemp -d "$tmp/moldK.XXXXXX"); mkdir -p "$sbK/.cargo"
cfgK="$sbK/.cargo/config.toml"
printf '[target.x86_64-unknown-linux-gnu]\nrustflags = ["-C", "target-cpu=native"]\n' >"$cfgK"
beforeK=$(cat "$cfgK")
outK=$(PATH="$stubA:$PATH" HOME="$sbK" CARGO_HOME="$sbK/.cargo" \
  bash "$BOOTSTRAP" --skip-smoke 2>&1)
if printf '%s' "$outK" | grep -q "existing \[target" \
   && [ "$beforeK" = "$(cat "$cfgK")" ] \
   && ! grep -q '^# BEGIN cqlite-mold' "$cfgK"; then
  ok "mold: pre-existing [target.<triple>] section -> warn, file byte-identical, no block"
else
  bad "mold: pre-existing target section not fail-safe (block written or file changed)"
  echo "--- config ---"; cat "$cfgK"; echo "--------------"
fi

# 6l. BOTH config files exist (blocker 1): cargo prefers the extension-less `config`,
#     so the block must land THERE, not in the ignored `config.toml`.
sbL=$(mktemp -d "$tmp/moldL.XXXXXX"); mkdir -p "$sbL/.cargo"
printf '[net]\nretry = 1\n' >"$sbL/.cargo/config"
printf '[net]\nretry = 2\n' >"$sbL/.cargo/config.toml"
tomlL_before=$(cat "$sbL/.cargo/config.toml")
PATH="$stubA:$PATH" HOME="$sbL" CARGO_HOME="$sbL/.cargo" \
  bash "$BOOTSTRAP" --skip-smoke >/dev/null 2>&1
if grep -q '^# BEGIN cqlite-mold' "$sbL/.cargo/config" \
   && ! grep -q '^# BEGIN cqlite-mold' "$sbL/.cargo/config.toml" \
   && [ "$tomlL_before" = "$(cat "$sbL/.cargo/config.toml")" ]; then
  ok "mold: both files present -> block lands in the effective 'config', config.toml untouched"
else
  bad "mold: both-files precedence wrong (block in the ignored config.toml)"
  echo "--- config ---"; cat "$sbL/.cargo/config"; echo "--- config.toml ---"; cat "$sbL/.cargo/config.toml"
fi

# 6m. pre-existing [build] rustflags (blocker 3): our target.rustflags would silently
#     disable it (first-match-wins), so bootstrap must WARN and write NOTHING.
sbM=$(mktemp -d "$tmp/moldM.XXXXXX"); mkdir -p "$sbM/.cargo"
cfgM="$sbM/.cargo/config.toml"
printf '[build]\nrustflags = ["-C", "target-cpu=native"]\n' >"$cfgM"
beforeM=$(cat "$cfgM")
outM=$(PATH="$stubA:$PATH" HOME="$sbM" CARGO_HOME="$sbM/.cargo" \
  bash "$BOOTSTRAP" --skip-smoke 2>&1)
if printf '%s' "$outM" | grep -q "existing \[build\] rustflags" \
   && [ "$beforeM" = "$(cat "$cfgM")" ] \
   && ! grep -q '^# BEGIN cqlite-mold' "$cfgM"; then
  ok "mold: pre-existing [build] rustflags -> warn, file byte-identical, no block"
else
  bad "mold: [build] rustflags not fail-safe (block written or file changed)"
  echo "--- config ---"; cat "$cfgM"; echo "--------------"
fi

# 6n. --yes INSTALLS then WIRES (blocker 4): the install stub places `mold` on PATH,
#     and the same run must re-detect it and write the managed block — one --yes run
#     delivers the full acceleration, not just the install. Runs a COPY of bootstrap in
#     a fake repo so the --yes dataset-fetch path is a fast no-op (no such script → no
#     network), never the real fetch-datasets.sh.
nrepo="$tmp/n-repo"; mkdir -p "$nrepo/scripts"
cp "$BOOTSTRAP" "$nrepo/scripts/bootstrap-agent-machine.sh"
sbN=$(mktemp -d "$tmp/moldN.XXXXXX"); mkdir -p "$sbN/.cargo"; stubN="$tmp/stubN"
mk_hermetic_bin "$stubN"
mk_stub "$stubN" cc 'exit 0'
mk_stub "$stubN" sudo 'exec "$@"'   # passthrough so `sudo apt-get …` runs the stub
# apt-get stub: on `install … mold`, drop a real `mold` executable onto PATH.
apt_body='installed=0; for a in "$@"; do [ "$a" = mold ] && installed=1; done; if [ "$installed" = 1 ]; then printf "#!/usr/bin/env bash\n[ \"\$1\" = --version ] && echo \"mold 2.4.0\"\nexit 0\n" > "'"$stubN/mold"'"; chmod +x "'"$stubN/mold"'"; fi; exit 0'
mk_stub "$stubN" apt-get "$apt_body"
PATH="$stubN" HOME="$sbN" CARGO_HOME="$sbN/.cargo" \
  bash "$nrepo/scripts/bootstrap-agent-machine.sh" --yes --skip-smoke >/dev/null 2>&1
if grep -q '^# BEGIN cqlite-mold' "$sbN/.cargo/config.toml" 2>/dev/null; then
  ok "mold: --yes installs mold then wires the managed block in the same run"
else
  bad "mold: --yes installed but never wired the linker config"
  ls -la "$sbN/.cargo" 2>/dev/null
fi

# 6i. the repo's committed .cargo/config.toml is never touched (blocker 7): run a COPY
#     of bootstrap whose BASH_SOURCE-derived REPO_ROOT is a fake repo that HAS a
#     .cargo/config.toml, with HOME/CARGO_HOME sandboxed elsewhere. The block must go
#     to the per-machine CARGO_HOME and the fake repo config must be byte-identical.
fakerepo="$tmp/fakerepo"; mkdir -p "$fakerepo/scripts" "$fakerepo/.cargo"
cp "$BOOTSTRAP" "$fakerepo/scripts/bootstrap-agent-machine.sh"
repo_cfg="$fakerepo/.cargo/config.toml"
printf '[registries.example]\nindex = "sparse+https://example.invalid/"\n' >"$repo_cfg"
repo_before=$(cat "$repo_cfg")
sbI=$(mktemp -d "$tmp/moldI.XXXXXX"); mkdir -p "$sbI/.cargo"
PATH="$stubA:$PATH" HOME="$sbI" CARGO_HOME="$sbI/.cargo" \
  bash "$fakerepo/scripts/bootstrap-agent-machine.sh" --skip-smoke >/dev/null 2>&1
if [ "$repo_before" = "$(cat "$repo_cfg")" ] \
   && grep -q '^# BEGIN cqlite-mold' "$sbI/.cargo/config.toml"; then
  ok "mold: repo-committed .cargo/config.toml untouched; block written to per-machine CARGO_HOME"
else
  bad "mold: repo config was mutated OR block did not land in CARGO_HOME"
  echo "--- repo cfg now ---"; cat "$repo_cfg"; echo "--------------------"
fi

# --- 7. git push credentials (issue #2942) ---------------------------------
# `gh` auth and `git` auth are SEPARATE credential paths: an authenticated gh CLI is
# NOT evidence that a raw `git push` can authenticate, and scripts/flow/claim.sh +
# claim-heartbeat.sh push with plain git on 10+ call sites. Every case below runs a
# COPY of bootstrap inside a throwaway git repo with a sandboxed HOME and its OWN
# GIT_CONFIG_GLOBAL, so the credential write under --yes can only ever land in the
# sandbox — never in this machine's real global git config.

# mk_fake_repo <dir> <origin-url>: a throwaway git repo holding a COPY of bootstrap
# at <dir>/scripts/, with `origin` set to <origin-url> and NO repo-local credential
# helper. The copy makes BASH_SOURCE-derived REPO_ROOT resolve to <dir>, so the
# credential probe reads THIS remote/config, never the real checkout's — and the
# --yes dataset fetch is a fast no-op (no test-data/scripts/fetch-datasets.sh here).
mk_fake_repo() {
  local dir="$1" url="$2"
  mkdir -p "$dir/scripts"
  cp "$BOOTSTRAP" "$dir/scripts/bootstrap-agent-machine.sh"
  git -c init.defaultBranch=main init -q "$dir" >/dev/null 2>&1
  git -C "$dir" remote add origin "$url" >/dev/null 2>&1
}

FAKE_TOKEN='ghp_FAKEtoken2942FAKEtoken2942FAKEtoken'

# 7a. HTTPS origin, NO credential helper anywhere, default (no --yes) mode ->
#     must WARN (never `ok`), print the identifying `could not read Username`
#     symptom + remediation, and write NOTHING.
sb7a=$(mktemp -d "$tmp/cred7a.XXXXXX"); stub7a="$tmp/stub7a"
mk_hermetic_bin "$stub7a"
repo7a="$tmp/repo7a"; mk_fake_repo "$repo7a" "https://github.com/pmcfadin/cqlite.git"
gc7a="$sb7a/gitconfig"   # deliberately absent
out7a=$(PATH="$stub7a" HOME="$sb7a" CARGO_HOME="$sb7a/.cargo" GIT_CONFIG_GLOBAL="$gc7a" \
  GH_TOKEN="" GITHUB_TOKEN="" bash "$repo7a/scripts/bootstrap-agent-machine.sh" --skip-smoke 2>&1)
if printf '%s' "$out7a" | grep -q "git push credentials"; then
  ok "cred: bootstrap emits the git-credential section"
else
  bad "cred: git-credential section MISSING from bootstrap output"
fi
if printf '%s' "$out7a" | grep -q "\[warn\].*git push" \
   && printf '%s' "$out7a" | grep -q "could not read Username" \
   && printf '%s' "$out7a" | grep -q "gh auth setup-git"; then
  ok "cred: no helper -> warn naming the 'could not read Username' symptom + remediation"
else
  bad "cred: no-helper case did not warn with the symptom/remediation"
  printf '%s\n' "$out7a" | grep -i -A3 "credential"
fi
if printf '%s' "$out7a" | grep -Eq '\[ok\].*(git push credentials|git credentials).*(resolve|configured)'; then
  bad "cred: reported OK for git push credentials while no helper is configured"
else
  ok "cred: authenticated gh alone is NOT reported as git push credentials"
fi
if [ ! -f "$gc7a" ]; then
  ok "cred: default (no --yes) run wrote NO global git config"
else
  bad "cred: default run wrote a global git config"; cat "$gc7a"
fi

# 7b. --yes with `gh auth setup-git` a no-op -> falls back to the $GH_TOKEN helper.
#     The config must carry the LITERAL `$GH_TOKEN` (dereferenced at call time) and
#     must NOT contain the token value; nothing the bootstrap wrote may contain it.
sb7b=$(mktemp -d "$tmp/cred7b.XXXXXX"); stub7b="$tmp/stub7b"
mk_hermetic_bin "$stub7b"
gh7b_log="$tmp/gh7b.log"; : >"$gh7b_log"
mk_stub "$stub7b" gh "echo \"\$*\" >>\"$gh7b_log\"; exit 0"   # setup-git succeeds but wires nothing
repo7b="$tmp/repo7b"; mk_fake_repo "$repo7b" "https://github.com/pmcfadin/cqlite.git"
gc7b="$sb7b/gitconfig"
out7b=$(PATH="$stub7b" HOME="$sb7b" CARGO_HOME="$sb7b/.cargo" GIT_CONFIG_GLOBAL="$gc7b" \
  GH_TOKEN="$FAKE_TOKEN" bash "$repo7b/scripts/bootstrap-agent-machine.sh" --yes --skip-smoke 2>&1)
if grep -q "auth setup-git" "$gh7b_log"; then
  ok "cred: --yes prefers 'gh auth setup-git' first"
else
  bad "cred: --yes never attempted 'gh auth setup-git'"
fi
if [ -f "$gc7b" ] && grep -q 'x-access-token' "$gc7b" && grep -qF 'GH_TOKEN' "$gc7b"; then
  ok "cred: --yes fell back to a helper that dereferences \$GH_TOKEN at call time"
else
  bad "cred: --yes did not configure the \$GH_TOKEN fallback helper"
  [ -f "$gc7b" ] && { echo "--- gitconfig ---"; cat "$gc7b"; echo "-----------------"; }
fi
# The helper MUST be host-scoped. A bare [credential] helper offers the GitHub token
# to every https host git talks to (submodules, cargo/pip git deps, a mistyped clone,
# anything answering 401) — and `gh auth setup-git`, the path this falls back FROM,
# scopes per host, so an unscoped fallback is strictly less safe than the preferred one.
if [ -f "$gc7b" ] \
   && git config --file "$gc7b" --get-all 'credential.https://github.com.helper' 2>/dev/null | grep -qF 'x-access-token' \
   && ! git config --file "$gc7b" --get-all credential.helper 2>/dev/null | grep -qF 'x-access-token'; then
  ok "cred: fallback helper is HOST-SCOPED (credential.https://github.com.helper), not a bare credential.helper"
else
  bad "cred: fallback helper is host-UNSCOPED — the token would be offered to every https host"
  [ -f "$gc7b" ] && { echo "--- gitconfig ---"; cat "$gc7b"; echo "-----------------"; }
fi
# The whole point of Decision 2: no file written by the bootstrap holds the secret.
leak7b=$(grep -rlF "$FAKE_TOKEN" "$sb7b" "$gc7b" "$repo7b" 2>/dev/null | head -5)
if [ -z "$leak7b" ]; then
  ok "cred: token VALUE never written to disk by the bootstrap"
else
  bad "cred: token value leaked into: $leak7b"
fi
if printf '%s' "$out7b" | grep -Eq '\[ok\].*git.*credential'; then
  ok "cred: --yes run reports the configured credential path as ok"
else
  bad "cred: --yes run never confirmed a working credential path"
  printf '%s\n' "$out7b" | grep -i -A2 "credential"
fi

# 7c. --yes where `gh auth setup-git` genuinely works -> use it, and do NOT also
#     add the $GH_TOKEN fallback helper (preferred form wins, Decision 2).
sb7c=$(mktemp -d "$tmp/cred7c.XXXXXX"); stub7c="$tmp/stub7c"
mk_hermetic_bin "$stub7c"
mk_stub "$stub7c" gh 'if [ "$1" = auth ] && [ "$2" = setup-git ]; then
  git config --global --add credential.helper "!f(){ test \"\$1\" = get || exit 0; echo username=gh-stub; echo password=stub-helper-secret; };f"
fi
exit 0'
repo7c="$tmp/repo7c"; mk_fake_repo "$repo7c" "https://github.com/pmcfadin/cqlite.git"
gc7c="$sb7c/gitconfig"
out7c=$(PATH="$stub7c" HOME="$sb7c" CARGO_HOME="$sb7c/.cargo" GIT_CONFIG_GLOBAL="$gc7c" \
  GH_TOKEN="$FAKE_TOKEN" bash "$repo7c/scripts/bootstrap-agent-machine.sh" --yes --skip-smoke 2>&1)
if [ -f "$gc7c" ] && grep -q 'gh-stub' "$gc7c" && ! grep -q 'x-access-token' "$gc7c" \
   && printf '%s' "$out7c" | grep -q "gh auth setup-git"; then
  ok "cred: a working 'gh auth setup-git' is preferred; no \$GH_TOKEN fallback added"
else
  bad "cred: working setup-git path did not win (fallback added or not reported)"
  [ -f "$gc7c" ] && { echo "--- gitconfig ---"; cat "$gc7c"; echo "-----------------"; }
fi

# 7d. SSH origin -> the https credential helper is irrelevant; report it and write
#     nothing, even under --yes.
sb7d=$(mktemp -d "$tmp/cred7d.XXXXXX"); stub7d="$tmp/stub7d"
mk_hermetic_bin "$stub7d"
repo7d="$tmp/repo7d"; mk_fake_repo "$repo7d" "git@github.com:pmcfadin/cqlite.git"
gc7d="$sb7d/gitconfig"
out7d=$(PATH="$stub7d" HOME="$sb7d" CARGO_HOME="$sb7d/.cargo" GIT_CONFIG_GLOBAL="$gc7d" \
  GH_TOKEN="$FAKE_TOKEN" bash "$repo7d/scripts/bootstrap-agent-machine.sh" --yes --skip-smoke 2>&1)
if printf '%s' "$out7d" | grep -qi "SSH" \
   && ! { [ -f "$gc7d" ] && grep -q 'x-access-token' "$gc7d"; }; then
  ok "cred: SSH origin reported as its own credential path; no helper written"
else
  bad "cred: SSH origin case wrote a helper or did not report the SSH path"
  [ -f "$gc7d" ] && cat "$gc7d"
fi

# 7f. FUNCTIONAL confinement of the config 7b actually produced: github.com gets a
#     credential, an unrelated host gets NOTHING. This is the assertion that would
#     have caught a bare [credential] helper regardless of how it was written.
cred_fill_host() {
  # cred_fill_host <config> <host> -> prints the resolved password line, if any
  printf 'protocol=https\nhost=%s\n\n' "$2" \
    | GIT_TERMINAL_PROMPT=0 GIT_ASKPASS=nonexistent-askpass SSH_ASKPASS=nonexistent-askpass \
      GIT_CONFIG_GLOBAL="$1" GIT_CONFIG_NOSYSTEM=1 GH_TOKEN="$FAKE_TOKEN" \
      git -C "$tmp" credential fill 2>/dev/null | grep '^password=.' || true
}
if [ -n "$(cred_fill_host "$gc7b" github.com)" ] \
   && [ -z "$(cred_fill_host "$gc7b" evil.example)" ] \
   && [ -z "$(cred_fill_host "$gc7b" gitlab.com)" ]; then
  ok "cred: helper answers for github.com and NOT for evil.example / gitlab.com"
else
  bad "cred: helper leaks the token to non-origin hosts (or fails for the origin host)"
fi

# 7g. Helper installed but GH_TOKEN absent from the environment — the reachable
#     production case, since --yes writes the helper GLOBALLY and PERSISTENTLY while
#     GH_TOKEN is per-shell (bootstrap interactively, then run the worker from
#     systemd/cron). git treats an empty `password=` as satisfied, so an
#     exit-status-only probe would report ok while every push fails.
sb7g=$(mktemp -d "$tmp/cred7g.XXXXXX"); stub7g="$tmp/stub7g"
mk_hermetic_bin "$stub7g"
repo7g="$tmp/repo7g"; mk_fake_repo "$repo7g" "https://github.com/pmcfadin/cqlite.git"
gc7g="$sb7g/gitconfig"
cp "$gc7b" "$gc7g" 2>/dev/null || :   # the exact helper config --yes produced in 7b
# Guard the guard: an EMPTY $gc7g would satisfy the warn assertion for the wrong
# reason (no helper at all), making this case vacuous.
if ! grep -q 'x-access-token' "$gc7g" 2>/dev/null; then
  bad "cred: 7g precondition FAILED — no helper installed, the warn below would be vacuous"
fi
out7g=$(PATH="$stub7g" HOME="$sb7g" CARGO_HOME="$sb7g/.cargo" GIT_CONFIG_GLOBAL="$gc7g" \
  GH_TOKEN="" GITHUB_TOKEN="" bash "$repo7g/scripts/bootstrap-agent-machine.sh" --skip-smoke 2>&1)
if printf '%s' "$out7g" | grep -q "\[warn\].*git push has NO credentials" \
   && ! printf '%s' "$out7g" | grep -Eq '\[ok\].*git push credentials resolve'; then
  ok "cred: helper present but GH_TOKEN unset -> WARN (a declining helper is not a credential)"
else
  bad "cred: empty-token case reported ok — probe accepted a non-answer"
  printf '%s\n' "$out7g" | grep -i -A2 "git push"
fi

# 7g-ii. The case the `^password=.` check exists for, which nothing covered: a helper
#        that ANSWERS with a literal EMPTY password line. git treats `password=` as
#        satisfied, so `git credential fill` exits 0 — an exit-status-only probe reports
#        a green machine on which every push fails. Our own helper declines instead of
#        emitting empty (7g), so without this case the non-empty check could be reverted
#        to an exit-status check with the suite still fully green.
sb7ge=$(mktemp -d "$tmp/cred7ge.XXXXXX"); stub7ge="$tmp/stub7ge"
mk_hermetic_bin "$stub7ge"
repo7ge="$tmp/repo7ge"; mk_fake_repo "$repo7ge" "https://github.com/pmcfadin/cqlite.git"
gc7ge="$sb7ge/gitconfig"
git config --file "$gc7ge" --add 'credential.https://github.com.helper' \
  '!f(){ test "$1" = get || exit 0; echo username=x-access-token; echo "password="; };f'
# Sanity: git itself considers this helper "satisfied" (exit 0) — that is the trap.
if printf 'protocol=https\nhost=github.com\n\n' \
   | GIT_CONFIG_GLOBAL="$gc7ge" GIT_CONFIG_NOSYSTEM=1 GIT_TERMINAL_PROMPT=0 \
     GIT_ASKPASS=nonexistent-askpass git -C "$tmp" credential fill >/dev/null 2>&1; then
  ok "cred: (precondition) git credential fill EXITS 0 on an empty password — the trap is real"
else
  bad "cred: (precondition) expected git to accept an empty password line"
fi
out7ge=$(PATH="$stub7ge" HOME="$sb7ge" CARGO_HOME="$sb7ge/.cargo" GIT_CONFIG_GLOBAL="$gc7ge" \
  GH_TOKEN="" GITHUB_TOKEN="" bash "$repo7ge/scripts/bootstrap-agent-machine.sh" --skip-smoke 2>&1)
if printf '%s' "$out7ge" | grep -q "\[warn\].*git push has NO credentials" \
   && ! printf '%s' "$out7ge" | grep -Eq '\[ok\].*git push credentials resolve'; then
  ok "cred: a helper answering with an EMPTY password is not accepted as a credential"
else
  bad "cred: empty-password helper reported ok — the probe trusted exit status"
  printf '%s\n' "$out7ge" | grep -i -A2 "git push"
fi

# 7h. A HANGING credential helper must not hang the bootstrap. Neither
#     GIT_TERMINAL_PROMPT nor GIT_ASKPASS governs a helper SUBPROCESS — real cases are
#     a Git Credential Manager device-code/browser flow, credential-cache waiting on a
#     dead daemon socket, and a locked osxkeychain. This matters beyond the operator:
#     section 3 above runs the real bootstrap against the real REPO_ROOT, so
#     `tooling-tests` probes a developer's ACTUAL helper chain and a hang there would
#     stall the gate of record. This is a deadlock/liveness guard, not a latency
#     budget: the helper sleeps 120s, the probe's own bound is 10s, and the outer
#     ceiling is 60s — ~6x slack, so host load can never flip it.
#     Resolution MUST match the script's (timeout || gtimeout): the fleet is macOS,
#     where GNU coreutils installs `gtimeout` — keying this case off `timeout` alone
#     would skip it on the one platform whose hang scenarios (locked osxkeychain, a GCM
#     browser flow) motivated the bound, leaving it uncovered exactly where it matters.
TIMEOUT_BIN_TEST="$(command -v timeout 2>/dev/null || command -v gtimeout 2>/dev/null || true)"
if [ -n "$TIMEOUT_BIN_TEST" ]; then
  sb7h=$(mktemp -d "$tmp/cred7h.XXXXXX"); stub7h="$tmp/stub7h"
  mk_hermetic_bin "$stub7h"
  repo7h="$tmp/repo7h"; mk_fake_repo "$repo7h" "https://github.com/pmcfadin/cqlite.git"
  gc7h="$sb7h/gitconfig"
  git config --file "$gc7h" --add 'credential.https://github.com.helper' '!f(){ sleep 120; };f'
  rc7h=0
  "$TIMEOUT_BIN_TEST" 60 env PATH="$stub7h" HOME="$sb7h" CARGO_HOME="$sb7h/.cargo" GIT_CONFIG_GLOBAL="$gc7h" \
    GH_TOKEN="" bash "$repo7h/scripts/bootstrap-agent-machine.sh" --skip-smoke >/dev/null 2>&1 || rc7h=$?
  if [ "$rc7h" -ne 124 ]; then
    ok "cred: a hanging credential helper is bounded — bootstrap still completes (rc=$rc7h)"
  else
    bad "cred: bootstrap HUNG on a blocking credential helper (killed at the outer ceiling)"
  fi
  # 7h-ii. The same guard on a STOCK-macOS-SHAPED host: GNU coreutils present only as
  #        `gtimeout`, no plain `timeout`. The fleet is macOS and two of the three hang
  #        scenarios are macOS-only, so a bound that resolves `timeout` alone is inert
  #        exactly where it is needed — and a Linux-only CI would never notice.
  sb7hm=$(mktemp -d "$tmp/cred7hm.XXXXXX"); stub7hm="$tmp/stub7hm"
  mk_hermetic_bin "$stub7hm"
  rm -f "$stub7hm/timeout"                          # <- the macOS shape
  ln -sf "$TIMEOUT_BIN_TEST" "$stub7hm/gtimeout"
  repo7hm="$tmp/repo7hm"; mk_fake_repo "$repo7hm" "https://github.com/pmcfadin/cqlite.git"
  gc7hm="$sb7hm/gitconfig"
  git config --file "$gc7hm" --add 'credential.https://github.com.helper' '!f(){ sleep 120; };f'
  rc7hm=0
  "$TIMEOUT_BIN_TEST" 60 env PATH="$stub7hm" HOME="$sb7hm" CARGO_HOME="$sb7hm/.cargo" \
    GIT_CONFIG_GLOBAL="$gc7hm" GH_TOKEN="" \
    bash "$repo7hm/scripts/bootstrap-agent-machine.sh" --skip-smoke >/dev/null 2>&1 || rc7hm=$?
  if [ "$rc7hm" -ne 124 ]; then
    ok "cred: the hang bound also applies on a gtimeout-only (macOS-shaped) host (rc=$rc7hm)"
  else
    bad "cred: bootstrap HUNG on a gtimeout-only host — the bound is inert on macOS"
  fi
else
  echo "skip - cred: hanging-helper guard needs timeout/gtimeout (neither on this host)"
fi

# 7e. Re-running --yes must not STACK a second copy of the helper. Bootstrap is
#     documented as idempotent, and a credential.helper list that grows every run
#     is a real footgun (git consults each entry in order).
sb7e=$(mktemp -d "$tmp/cred7e.XXXXXX"); stub7e="$tmp/stub7e"
mk_hermetic_bin "$stub7e"
mk_stub "$stub7e" gh 'exit 0'   # setup-git is a no-op -> the fallback helper is used
repo7e="$tmp/repo7e"; mk_fake_repo "$repo7e" "https://github.com/pmcfadin/cqlite.git"
gc7e="$sb7e/gitconfig"
for _ in 1 2; do
  PATH="$stub7e" HOME="$sb7e" CARGO_HOME="$sb7e/.cargo" GIT_CONFIG_GLOBAL="$gc7e" \
    GH_TOKEN="$FAKE_TOKEN" bash "$repo7e/scripts/bootstrap-agent-machine.sh" --yes --skip-smoke >/dev/null 2>&1
done
out7e=$(PATH="$stub7e" HOME="$sb7e" CARGO_HOME="$sb7e/.cargo" GIT_CONFIG_GLOBAL="$gc7e" \
  GH_TOKEN="$FAKE_TOKEN" bash "$repo7e/scripts/bootstrap-agent-machine.sh" --yes --skip-smoke 2>&1)
helper_count=$(grep -c 'x-access-token' "$gc7e" 2>/dev/null); helper_count="${helper_count:-0}"
if [ "$helper_count" = 1 ]; then
  ok "cred: repeated --yes runs keep exactly one credential helper (idempotent)"
else
  bad "cred: helper stacked across re-runs (count=$helper_count)"
  [ -f "$gc7e" ] && cat "$gc7e"
fi
# On the re-run the probe SUCCEEDS, so the verdict comes from the ok branch — and its
# advisories must see the HOST-SCOPED key this script itself writes. A bare
# `credential.helper` lookup would go silent on exactly the config it just created,
# muting the caveat that matters most to a systemd/cron worker.
if printf '%s' "$out7e" | grep -q 'reads \$GH_TOKEN from the ENVIRONMENT'; then
  ok "cred: env-dependency caveat fires for the HOST-SCOPED helper the script writes"
else
  bad "cred: env-dependency caveat missed a host-scoped helper"
  printf '%s\n' "$out7e" | grep -i -A2 "git push credentials"
fi

# 7i. Same blind spot on the other advisory: a host-scoped helper at REPO-LOCAL scope
#     with no global one must still raise the "a fresh clone won't inherit it" note.
sb7i=$(mktemp -d "$tmp/cred7i.XXXXXX"); stub7i="$tmp/stub7i"
mk_hermetic_bin "$stub7i"
repo7i="$tmp/repo7i"; mk_fake_repo "$repo7i" "https://github.com/pmcfadin/cqlite.git"
git -C "$repo7i" config --local --add 'credential.https://github.com.helper' \
  '!f(){ test "$1" = get || exit 0; echo username=x; echo password=local-only-secret; };f'
out7i=$(PATH="$stub7i" HOME="$sb7i" CARGO_HOME="$sb7i/.cargo" GIT_CONFIG_GLOBAL="$sb7i/gitconfig" \
  GH_TOKEN="" bash "$repo7i/scripts/bootstrap-agent-machine.sh" --skip-smoke 2>&1)
if printf '%s' "$out7i" | grep -q 'REPO-LOCAL scope only'; then
  ok "cred: repo-local-scope note fires for a HOST-SCOPED local helper"
else
  bad "cred: repo-local-scope note missed a host-scoped local helper"
  printf '%s\n' "$out7i" | grep -i -A3 "git push credentials"
fi

# --- 8. Board check is a FUNCTIONAL, READ-ONLY probe (issue #2942) ----------
# The false OK this exists to prevent: a token whose scopes INCLUDE `project` while
# `gh project` still fails for a missing `read:org`, and the equivalent
# `updateProjectV2ItemFieldValue` GraphQL mutation succeeds with the SAME token. A
# scope-string match therefore proves nothing about the operation, and must never be
# the verdict.

# mk_board_gh <dir> <log> <scopes> <missing-scopes|""> <gh-project-rc> <gh-api-rc>
mk_board_gh() {
  local dir="$1" log="$2" scopes="$3" missing="$4" prc="$5" arc="$6"
  local missing_echo=""
  [ -n "$missing" ] && missing_echo="echo \"  ! Missing required token scopes: $missing\""
  cat >"$dir/gh" <<EOF
#!/usr/bin/env bash
echo "\$*" >>"$log"
case "\$1" in
  auth)
    if [ "\$2" = status ]; then
      echo "github.com"
      echo "  ✓ Logged in to github.com account tester (GH_TOKEN)"
      echo "  - Token scopes: $scopes"
      $missing_echo
    fi
    exit 0 ;;
  project) exit $prc ;;
  # The GraphQL probe demands a NON-EMPTY project id — `gh api graphql` exits 0 on a
  # query that RESOLVES TO NULL, so an exit code alone would be a false OK. api-rc
  # 'null' simulates exactly that: a clean exit carrying no project.
  api)
    if [ "$arc" = null ]; then exit 0; fi
    [ "$arc" = 0 ] && echo "PVT_kwStubProjectId"
    exit "$arc" ;;
  *)       exit 0 ;;
esac
EOF
  chmod +x "$dir/gh"
}

# run_board_case <name> <scopes> <missing> <project-rc> <api-rc> -> sets BOARD_OUT/BOARD_LOG
# CQLITE_PROJECT_ACCOUNT is pinned to the stub's account so these cases exercise the
# VERDICT logic with no account switch in play (switching has its own cases below).
run_board_case() {
  local name="$1" scopes="$2" missing="$3" prc="$4" arc="$5"
  local sb stub repo
  sb=$(mktemp -d "$tmp/board-$name.XXXXXX"); stub="$tmp/stub-board-$name"
  mk_hermetic_bin "$stub"
  BOARD_LOG="$tmp/gh-board-$name.log"; : >"$BOARD_LOG"
  mk_board_gh "$stub" "$BOARD_LOG" "$scopes" "$missing" "$prc" "$arc"
  repo="$tmp/repo-board-$name"; mk_fake_repo "$repo" "https://github.com/pmcfadin/cqlite.git"
  BOARD_OUT=$(PATH="$stub" HOME="$sb" CARGO_HOME="$sb/.cargo" GIT_CONFIG_GLOBAL="$sb/gitconfig" \
    CQLITE_PROJECT_ACCOUNT=tester CQLITE_PROJECT_NUMBER=1 \
    GH_TOKEN="" bash "$repo/scripts/bootstrap-agent-machine.sh" --skip-smoke 2>&1)
}

# run_board_auth_case <name> <auth-status-body> [env...] -> BOARD_OUT/BOARD_LOG
# Like run_board_case but the caller supplies the VERBATIM `gh auth status` body, so a
# multi-account host can be modelled exactly. `gh project`/`gh api` always succeed, so
# any non-green verdict is attributable purely to which account's stanza was parsed.
run_board_auth_case() {
  local name="$1" body="$2"; shift 2
  local sb stub repo
  sb=$(mktemp -d "$tmp/bauth-$name.XXXXXX"); stub="$tmp/stub-bauth-$name"
  mk_hermetic_bin "$stub"
  BOARD_LOG="$tmp/gh-bauth-$name.log"; : >"$BOARD_LOG"
  printf '%s\n' "$body" >"$tmp/authbody-$name.txt"
  cat >"$stub/gh" <<EOF
#!/usr/bin/env bash
echo "\$*" >>"$BOARD_LOG"
case "\$1" in
  auth) [ "\$2" = status ] && cat "$tmp/authbody-$name.txt"; exit 0 ;;
  project) exit 0 ;;
  api)     echo "PVT_kwStubProjectId"; exit 0 ;;
  *)       exit 0 ;;
esac
EOF
  chmod +x "$stub/gh"
  repo="$tmp/repo-bauth-$name"; mk_fake_repo "$repo" "https://github.com/pmcfadin/cqlite.git"
  # Caller overrides go through `env`: a VAR=value coming from "$@" is the result of an
  # expansion, so bash would treat it as a COMMAND NAME, not an assignment — the
  # override would silently do nothing and the case would assert against the default.
  BOARD_OUT=$(PATH="$stub" HOME="$sb" CARGO_HOME="$sb/.cargo" GIT_CONFIG_GLOBAL="$sb/gitconfig" \
    CQLITE_PROJECT_NUMBER=1 \
    GH_TOKEN="" env "$@" bash "$repo/scripts/bootstrap-agent-machine.sh" --skip-smoke 2>&1)
}

# 8a. THE false-OK case: `project` scope present, `read:org` missing, `gh project`
#     unusable, GraphQL fine. Today's scope-string check prints an unqualified
#     "board dispatch works" here — that verdict must be impossible.
run_board_case falseok "'project', 'repo', 'workflow'" "'read:org'" 1 0
if printf '%s' "$BOARD_OUT" | grep -q "board dispatch works"; then
  bad "board: scope-present-but-gh-project-unusable STILL prints 'board dispatch works'"
else
  ok "board: scope present + gh project unusable -> no unqualified 'board dispatch works'"
fi
if printf '%s' "$BOARD_OUT" | grep -q "updateProjectV2ItemFieldValue"; then
  ok "board: names the updateProjectV2ItemFieldValue GraphQL write fallback"
else
  bad "board: never named the updateProjectV2ItemFieldValue fallback"
  printf '%s\n' "$BOARD_OUT" | grep -i -A3 "board"
fi
if printf '%s' "$BOARD_OUT" | grep -qi "read:org"; then
  ok "board: surfaces the read:org scope gap gh itself reports"
else
  bad "board: did not surface the read:org gap"
fi

# 8b. `gh project` READ works but gh still reports missing required scopes — the
#     write (`item-edit`) can fail. Still not an unqualified success.
run_board_case partial "'project', 'repo', 'workflow'" "'read:org'" 0 0
if ! printf '%s' "$BOARD_OUT" | grep -q "board dispatch works" \
   && printf '%s' "$BOARD_OUT" | grep -q "updateProjectV2ItemFieldValue"; then
  ok "board: read-OK + missing required scopes -> qualified verdict naming the fallback"
else
  bad "board: read-OK + missing scopes reported as unqualified success"
  printf '%s\n' "$BOARD_OUT" | grep -i -A3 "board"
fi

# 8c. Fully healthy token: probe succeeds, gh reports no missing scopes -> an ok.
#     The assertion names PROBE-DERIVED text on purpose: a looser `[ok].*board` also
#     matches the old scope-string verdict ("[ok] 'project' scope present — board
#     dispatch works"), so it would pass against the very bug this section exists to
#     catch. In a change about false OKs, a test that passes against the bug is the
#     one thing that must not ship.
run_board_case healthy "'project', 'read:org', 'repo', 'workflow'" "" 0 0
if printf '%s' "$BOARD_OUT" | grep -Eq "\[ok\].*board #1 \(pmcfadin\) reachable.*read probe OK"; then
  ok "board: healthy token reports ok with a PROBE-derived verdict"
else
  bad "board: healthy token did not produce a probe-derived ok verdict"
  printf '%s\n' "$BOARD_OUT" | grep -i -A3 "board #"
fi

# 8d. Unreachable board (both probes fail) -> a loud warn, never a scope-based pass.
run_board_case unreachable "'project', 'repo', 'workflow'" "" 1 1
if printf '%s' "$BOARD_OUT" | grep -Eq '\[warn\].*board' \
   && ! printf '%s' "$BOARD_OUT" | grep -q "board dispatch works"; then
  ok "board: both probes failing -> warn (scope match never rescues the verdict)"
else
  bad "board: unreachable board did not warn"
  printf '%s\n' "$BOARD_OUT" | grep -i -A3 "board"
fi

# 8f. GraphQL exits 0 but resolves to NO project (wrong owner kind / wrong number).
#     An exit-code-only probe would call that reachable — it must not.
run_board_case nullproject "'project', 'repo', 'workflow'" "" 1 null
if printf '%s' "$BOARD_OUT" | grep -Eq '\[warn\].*(UNREACHABLE|BOTH probes failed)'; then
  ok "board: GraphQL exit 0 with a null project counts as a FAILED probe, not reachable"
else
  bad "board: null-project GraphQL reply was treated as a working fallback"
  printf '%s\n' "$BOARD_OUT" | grep -i -A3 "board #"
fi

# 8g. READ-ONLY project access ('read:project', no 'project') with a clean probe and
#     no gh-reported missing scopes. Board WRITES — the whole dispatch loop — still
#     fail, so an unqualified ok as the section's LAST word would be a false OK even
#     though an earlier line warned about the scope.
run_board_case readonlyscope "'read:project', 'repo', 'workflow'" "" 0 0
if printf '%s' "$BOARD_OUT" | grep -Eq '\[ok\].*board #1.*reachable'; then
  bad "board: read-only project scope still printed an unqualified 'reachable' ok"
elif printf '%s' "$BOARD_OUT" | grep -Eq "\[warn\].*board READ works.*'project' WRITE scope is MISSING"; then
  ok "board: read-only project scope -> READ-works warn naming the missing WRITE scope"
else
  bad "board: read-only project scope produced neither the ok nor the expected warn"
  printf '%s\n' "$BOARD_OUT" | grep -i -A2 "board"
fi

# --- 8h. The verdict must be attributed to the ACTIVE account ----------------
# `gh auth status` prints one stanza PER logged-in account and the active one is not
# guaranteed first, so a whole-output grep can read a DIFFERENT account's scopes than
# the one every gh call uses. This repo documents the exact hazard
# (.claude/skills/flow-board/SKILL.md): the active account silently flips to an EMU
# account lacking `project`, and board writes then degrade SILENTLY. Both cases below
# are built so a whole-output grep gives the WRONG verdict.

# 8h-i. ACTIVE account is clean; a NON-active account reports missing scopes. A
#       whole-output grep sees that stray line and wrongly qualifies the verdict.
run_board_auth_case active-clean 'github.com
  ✓ Logged in to github.com account other-emu (keyring)
  - Active account: false
  - Token scopes: '"'"'project'"'"', '"'"'repo'"'"'
  ! Missing required token scopes: '"'"'read:org'"'"'
  ✓ Logged in to github.com account pmcfadin (keyring)
  - Active account: true
  - Token scopes: '"'"'project'"'"', '"'"'read:org'"'"', '"'"'repo'"'"''
if printf '%s' "$BOARD_OUT" | grep -Eq "\[ok\].*board #1.*reachable as 'pmcfadin'"; then
  ok "board: a NON-active account's missing-scopes line does not qualify the verdict"
else
  bad "board: verdict read a non-active account's stanza"
  printf '%s\n' "$BOARD_OUT" | grep -i -A2 "board\|account"
fi

# 8h-ii. A NON-active account listed FIRST has 'project'; the ACTIVE one does not. A
#        whole-output `grep 'Token scopes:' | head -1` picks the wrong stanza and would
#        greenlight a machine whose dispatch writes all fail.
run_board_auth_case active-noproject 'github.com
  ✓ Logged in to github.com account other-emu (keyring)
  - Active account: false
  - Token scopes: '"'"'project'"'"', '"'"'read:org'"'"', '"'"'repo'"'"'
  ✓ Logged in to github.com account pmcfadin (keyring)
  - Active account: true
  - Token scopes: '"'"'read:project'"'"', '"'"'repo'"'"''
if ! printf '%s' "$BOARD_OUT" | grep -Eq '\[ok\].*board #1.*reachable' \
   && printf '%s' "$BOARD_OUT" | grep -q "'project' scope MISSING on gh account 'pmcfadin'"; then
  ok "board: scopes are read from the ACTIVE stanza, not the first one printed"
else
  bad "board: scopes were read from a non-active (first-listed) account"
  printf '%s\n' "$BOARD_OUT" | grep -i -A2 "scope\|board #"
fi

# 8h-iii. The operator must be able to see WHICH account the verdict is about.
if printf '%s' "$BOARD_OUT" | grep -q "measuring gh account 'pmcfadin'"; then
  ok "board: names the account the verdict is about"
else
  bad "board: verdict does not name the account it measured"
fi

# --- 8i. Probe the account board dispatch actually uses ----------------------
# flow-board forces CQLITE_PROJECT_ACCOUNT active before EVERY board op. Probing as
# whatever happens to be active measures a different identity: with an EMU account
# active, bootstrap would shout "board UNREACHABLE — a session must STOP" about a
# machine where flow-board switches and works fine. Mirroring the switch is required —
# and because it mutates real gh state, the operator's account must be RESTORED.
mk_switch_gh() {
  # mk_switch_gh <dir> <log> <statefile> <acctA> <acctB>  (acctA starts active)
  local dir="$1" log="$2" state="$3" a="$4" b="$5"
  printf '%s' "$a" >"$state"
  cat >"$dir/gh" <<EOF
#!/usr/bin/env bash
echo "\$*" >>"$log"
cur=\$(cat "$state" 2>/dev/null)
case "\$1" in
  auth)
    case "\$2" in
      status)
        echo "github.com"
        for acct in $a $b; do
          echo "  ✓ Logged in to github.com account \$acct (keyring)"
          if [ "\$acct" = "\$cur" ]; then echo "  - Active account: true"
          else echo "  - Active account: false"; fi
          echo "  - Token scopes: 'project', 'read:org', 'repo'"
        done
        exit 0 ;;
      switch)
        shift 2
        while [ \$# -gt 0 ]; do
          [ "\$1" = --user ] && printf '%s' "\$2" >"$state"
          shift
        done
        exit 0 ;;
      *) exit 0 ;;
    esac ;;
  project) exit 0 ;;
  api)     echo "PVT_kwStubProjectId"; exit 0 ;;
  *)       exit 0 ;;
esac
EOF
  chmod +x "$dir/gh"
}

sb8i=$(mktemp -d "$tmp/board8i.XXXXXX"); stub8i="$tmp/stub8i"
mk_hermetic_bin "$stub8i"
log8i="$tmp/gh8i.log"; : >"$log8i"; state8i="$tmp/gh8i.state"
mk_switch_gh "$stub8i" "$log8i" "$state8i" other-emu pmcfadin   # EMU active at start
repo8i="$tmp/repo8i"; mk_fake_repo "$repo8i" "https://github.com/pmcfadin/cqlite.git"
out8i=$(PATH="$stub8i" HOME="$sb8i" CARGO_HOME="$sb8i/.cargo" GIT_CONFIG_GLOBAL="$sb8i/gitconfig" \
  CQLITE_PROJECT_NUMBER=1 GH_TOKEN="" bash "$repo8i/scripts/bootstrap-agent-machine.sh" --skip-smoke 2>&1)
if grep -q -- 'auth switch --user pmcfadin' "$log8i"; then
  ok "board: switches to CQLITE_PROJECT_ACCOUNT before probing (mirrors flow-board)"
else
  bad "board: never switched to the board account — probes a different identity than dispatch uses"
fi
if grep -q -- 'auth switch --user other-emu' "$log8i" && [ "$(cat "$state8i")" = other-emu ]; then
  ok "board: RESTORES the operator's active account after the probe (a check must not mutate)"
else
  bad "board: left the active account switched to '$(cat "$state8i")' — a check mutated host state"
fi
if printf '%s' "$out8i" | grep -Eq '\[ok\].*board #1.*reachable' ; then
  ok "board: reports reachable for the account dispatch actually uses"
else
  bad "board: did not reach a green verdict after switching to the board account"
  printf '%s\n' "$out8i" | grep -i -A2 "board #"
fi

# 8j. With an env token, gh ignores the keyring and `gh auth switch` cannot change the
#     identity — attempting it would be theatre, and mutating host state for a no-op
#     is worse than not trying.
sb8j=$(mktemp -d "$tmp/board8j.XXXXXX"); stub8j="$tmp/stub8j"
mk_hermetic_bin "$stub8j"
log8j="$tmp/gh8j.log"; : >"$log8j"; state8j="$tmp/gh8j.state"
mk_switch_gh "$stub8j" "$log8j" "$state8j" other-emu pmcfadin
repo8j="$tmp/repo8j"; mk_fake_repo "$repo8j" "https://github.com/pmcfadin/cqlite.git"
out8j=$(PATH="$stub8j" HOME="$sb8j" CARGO_HOME="$sb8j/.cargo" GIT_CONFIG_GLOBAL="$sb8j/gitconfig" \
  CQLITE_PROJECT_NUMBER=1 GH_TOKEN="$FAKE_TOKEN" bash "$repo8j/scripts/bootstrap-agent-machine.sh" --skip-smoke 2>&1)
if ! grep -q -- 'auth switch' "$log8j" && [ "$(cat "$state8j")" = other-emu ]; then
  ok "board: an env token suppresses the switch entirely (no pointless host mutation)"
else
  bad "board: attempted an account switch while GH_TOKEN was in force"
fi
if printf '%s' "$out8j" | grep -q "from GH_TOKEN in the environment"; then
  ok "board: names the env token as the identity source"
else
  bad "board: did not disclose that the identity came from GH_TOKEN"
fi

# --- 8k. CQLITE_PROJECT_NUMBER unset is a DISPATCH BLOCKER -------------------
# flow-board reads `${CQLITE_PROJECT_NUMBER:-}` and STOPs when it is empty. A bootstrap
# that defaulted the number to a guess would print a green "board reachable" on a box
# where every flow-* skill refuses to dispatch — the same false green, one layer out.
# 8k-i: the board is discoverable by title -> warn naming the exact export line.
sb8k=$(mktemp -d "$tmp/board8k.XXXXXX"); stub8k="$tmp/stub8k"
mk_hermetic_bin "$stub8k"
jqp=$(type -P jq 2>/dev/null) && ln -sf "$jqp" "$stub8k/jq"
cat >"$stub8k/gh" <<'EOF'
#!/usr/bin/env bash
case "$1" in
  auth) [ "$2" = status ] && { echo "github.com"
        echo "  ✓ Logged in to github.com account tester (keyring)"
        echo "  - Active account: true"
        echo "  - Token scopes: 'project', 'read:org', 'repo'"; }; exit 0 ;;
  project)
    [ "$2" = list ] && { echo '{"projects":[{"title":"CQLite Delivery","number":7}]}'; exit 0; }
    exit 0 ;;
  api) echo "PVT_kwStubProjectId"; exit 0 ;;
  *)   exit 0 ;;
esac
EOF
chmod +x "$stub8k/gh"
repo8k="$tmp/repo8k"; mk_fake_repo "$repo8k" "https://github.com/pmcfadin/cqlite.git"
out8k=$(PATH="$stub8k" HOME="$sb8k" CARGO_HOME="$sb8k/.cargo" GIT_CONFIG_GLOBAL="$sb8k/gitconfig" \
  CQLITE_PROJECT_ACCOUNT=tester GH_TOKEN="" bash "$repo8k/scripts/bootstrap-agent-machine.sh" --skip-smoke 2>&1)
if printf '%s' "$out8k" | grep -Eq '\[ok\].*board #.*reachable'; then
  bad "board: unexported CQLITE_PROJECT_NUMBER still produced a green 'reachable' verdict"
elif printf '%s' "$out8k" | grep -q 'CQLITE_PROJECT_NUMBER is NOT exported'; then
  ok "board: unexported CQLITE_PROJECT_NUMBER is reported as a dispatch blocker"
else
  bad "board: unexported CQLITE_PROJECT_NUMBER neither warned nor blocked the green verdict"
  printf '%s\n' "$out8k" | grep -i -A2 "board"
fi
if [ -n "$jqp" ]; then
  if printf '%s' "$out8k" | grep -q 'export CQLITE_PROJECT_NUMBER=7'; then
    ok "board: discovers the number by title and prints the exact export line"
  else
    bad "board: did not resolve the board by title / print the export line"
    printf '%s\n' "$out8k" | grep -i "PROJECT_NUMBER"
  fi
else
  echo "skip - board: title discovery needs jq (absent on this host)"
fi

# 8k-ii: not discoverable -> point at setup-project-board.sh, still no green.
run_board_auth_case nonumber 'github.com
  ✓ Logged in to github.com account pmcfadin (keyring)
  - Active account: true
  - Token scopes: '"'"'project'"'"', '"'"'read:org'"'"', '"'"'repo'"'"'' CQLITE_PROJECT_NUMBER=
if ! printf '%s' "$BOARD_OUT" | grep -Eq '\[ok\].*board #.*reachable' \
   && printf '%s' "$BOARD_OUT" | grep -q 'setup-project-board.sh'; then
  ok "board: unresolvable board number -> no green, points at setup-project-board.sh"
else
  bad "board: unresolvable board number did not block the green verdict"
  printf '%s\n' "$BOARD_OUT" | grep -i -A2 "board"
fi

# 8l. The account restore must be armed by a TRAP, not just an inline block: two
#     network calls sit between the switch and the restore, so an interrupt or a
#     supervisor SIGTERM in that window would strand the operator's active account.
if grep -q "trap 'restore_board_account' EXIT" "$BOOTSTRAP" \
   && grep -q "trap 'restore_board_account; exit 130' INT" "$BOOTSTRAP" \
   && grep -q "trap 'restore_board_account; exit 143' TERM" "$BOOTSTRAP"; then
  ok "board: account restore is armed on EXIT/INT/TERM, not only the happy path"
else
  bad "board: no EXIT/INT/TERM trap arming the account restore"
fi
# ...and the probes it brackets must be BOUNDED, so the window cannot hang open.
if grep -q 'bounded 20 gh project view' "$BOOTSTRAP" \
   && grep -q 'bounded 20 gh api graphql' "$BOOTSTRAP"; then
  ok "board: both probes inside the switch/restore bracket are time-bounded"
else
  bad "board: an unbounded probe sits between the account switch and its restore"
fi

# 8e. The probe is READ-ONLY: across EVERY board case above, the bootstrap must never
#     have invoked a board-mutating gh call. The glob covers all three log families —
#     the identity-switching cases most of all, where a mutating call would matter most.
mutating=$(cat "$tmp"/gh-board-*.log "$tmp"/gh-bauth-*.log "$tmp"/gh8i.log "$tmp"/gh8j.log 2>/dev/null \
  | grep -Ei 'item-edit|item-add|item-delete|item-archive|--field|mutation' | head -5)
if [ -z "$mutating" ]; then
  ok "board: probe never invoked a mutating gh/board operation"
else
  bad "board: probe issued a MUTATING call: $mutating"
fi

echo
echo "PASS=$PASS FAIL=$FAIL"
[ "$FAIL" -eq 0 ]
