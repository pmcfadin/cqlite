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

# Run with the shims FIRST on PATH, default mode (no --yes), skipping the smoke.
run_out=$(PATH="$tmp:$PATH" bash "$BOOTSTRAP" --skip-smoke 2>&1); run_rc=$?

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
guard_out=$(PATH="$tmp:/usr/bin:/bin" bash "$BOOTSTRAP" --skip-smoke 2>&1)
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

echo
echo "PASS=$PASS FAIL=$FAIL"
[ "$FAIL" -eq 0 ]
