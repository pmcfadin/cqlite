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
           head tail tr sort cut wc stat env git find xargs basename date sleep expr; do
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

echo
echo "PASS=$PASS FAIL=$FAIL"
[ "$FAIL" -eq 0 ]
