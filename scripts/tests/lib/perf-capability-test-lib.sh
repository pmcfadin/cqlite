#!/usr/bin/env bash
# shellcheck shell=bash
# Shared harness for the PERF PROFILING CAPABILITY suites (issue #3249):
#   scripts/tests/test_perf_capability.sh            — the helper's own contract
#   scripts/tests/test_perf_capability_bootstrap.sh  — the bootstrap section end-to-end
#
# WHY A LIB. The two suites were one 1333-line file, over the ~800/~1500 campsite
# targets and still growing a case per review round. They are split by RESPONSIBILITY
# (helper unit contract vs bootstrap integration), and everything both need lives here
# ONCE so the identity/platform stubs and the host-safety asserts can never drift apart
# between them.
#
# HOST SAFETY IS THE POINT OF THIS FILE. Nothing in either suite may touch the real
# /etc/sysctl.d or /proc: the two test-only env seams stand in (CQLITE_PERF_PROC_DIR,
# CQLITE_PERF_SYSCTL_DIR), every privileged/mutating tool is a recording PATH shim, and
# `perf_test_assert_host_clean` asserts the mutation-freedom directly at the end of each
# suite. Since #3249 review R4-3 the seams are MANDATORY under the marker — test mode
# refuses to fall back to a production directory — so a case that forgets one fails
# closed and loudly instead of writing the host's real drop-in.
#
# Sourced, never executed. The sourcing suite must NOT have `set -e` (the cases test
# failing commands on purpose); `set -uo pipefail` is what both use.

PERF_TEST_LIB_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SCRIPT_DIR=$(cd "$PERF_TEST_LIB_DIR/.." && pwd)          # scripts/tests
BOOTSTRAP="$SCRIPT_DIR/../bootstrap-agent-machine.sh"
PERFLIB="$SCRIPT_DIR/../perf-capability.sh"

PASS=0
FAIL=0
ok()  { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad() { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }

# sudo_perf_offenders <tripwire-log>: the recorded `sudo` lines belonging to the PERF
# path (its `-n` availability probe, its `tee`, its `sysctl`) that do NOT carry `-n`.
# `-n` is what makes bootstrap unpromptable on an unattended worker, so dropping it
# from PERF_ROOT is a defect every functional assert would still pass. Lines from other
# bootstrap sections (e.g. the mold `sudo apt-get install`) are out of this issue's scope.
sudo_perf_offenders() {
  grep -E '^sudo ' "$1" 2>/dev/null | grep -E '(\btee\b|\bsysctl\b|\btrue\b)' | grep -v '^sudo -n ' || true
}

tmp=$(mktemp -d "${TMPDIR:-/tmp}/perf-cap-test.XXXXXX")
trap 'rm -rf "$tmp"' EXIT

# Global-state isolation, same posture as test_bootstrap_agent_machine.sh: the
# bootstrap runs below read/write git config and read board env, and these suites run
# inside `tooling-tests` on the very box hosting a live delivery session.
export GIT_CONFIG_GLOBAL="$tmp/global-gitconfig"
export GIT_CONFIG_NOSYSTEM=1
: >"$GIT_CONFIG_GLOBAL"
unset CQLITE_PROJECT_NUMBER CQLITE_PROJECT_OWNER CQLITE_PROJECT_ACCOUNT PROJECT_TITLE
# A worker shell may export the seams themselves; a test must set exactly what it means.
unset CQLITE_PERF_PROC_DIR CQLITE_PERF_SYSCTL_DIR
# ...and so may a `sudo bash scripts/tests/...` invocation export SUDO_UID/GID/USER,
# which the privilege-drop target resolution reads. A case that means to exercise that
# path sets them itself; inheriting them would make the suite's verdict depend on how
# it was launched (the same env-inheritance class as PERF_SECTION_OK).
unset SUDO_UID SUDO_GID SUDO_USER
# The section under test must never be steered by an ambient export either: bootstrap
# initialises PERF_SECTION_OK itself, and the bootstrap suite proves it.
unset PERF_SECTION_OK
# The two path seams are INERT unless this marker is set; under the marker they are
# MANDATORY, must be absolute and non-production, and a real sudo/sysctl on PATH is a
# hard refusal (the shim dir is declared per case in CQLITE_PERF_TEST_PRIV_DIR). Cases
# that must exercise the PRODUCTION defaults run with `env -u CQLITE_PERF_TEST_MODE`.
export CQLITE_PERF_TEST_MODE=1

# mkuname <dir> <sysname>: a `uname` stub, so the Linux/Darwin branch under test is
# the one selected by the CASE, not by whatever host runs the suite. Without this the
# whole suite was silently Linux-host-only: on a Darwin gate host every bootstrap run
# would take the "nothing to configure on macos" path and 10 cases would fail.
# `-r`/`-m` answer too, because bootstrap uses them elsewhere.
mkuname() {
  # rm FIRST: several dirs below `ln -sf` the real uname into place, and writing
  # through that symlink would clobber the host's /usr/bin/uname.
  rm -f "$1/uname"
  cat >"$1/uname" <<EOF
#!/usr/bin/env bash
case "\${1:-}" in
  -r) echo 6.0.0-cqlite-test ;;
  -m) echo x86_64 ;;
  *)  echo $2 ;;
esac
EOF
  chmod +x "$1/uname"
}

# mkid <dir> <self-uid> [<other-uid>]: an `id` stub, so the ROOT / NON-ROOT branch
# under test is the one selected by the CASE, not by whatever UID runs the suite.
# Without this the suite was silently NON-ROOT-runner-only: nearly every case asserts
# the unprivileged behaviour (a `sudo -n true` probe, a `sudo -n tee`, a printed `sudo`
# remedy), so running the whole thing as root — a container, a CI image, anyone's
# `sudo bash` — took the root branch and FAILED, reddening the mandatory
# `tooling-tests` gate component. Same class as the Linux-host-only bug mkuname fixes,
# and the same `rm -f` first: several dirs below `ln -sf` the real `id` into place, and
# writing through that symlink would clobber the host's /usr/bin/id.
#
# <other-uid> answers a USER OPERAND (`id -u nobody` / `id -g nobody`), which the
# privilege-drop target resolution asks for: a shim that answered its OWN uid there
# would let a root case "resolve" root as the unprivileged probe identity — the exact
# false verification the drop exists to prevent. Omitted => the operand form FAILS
# (that box has no such account), which is the honest answer for a case that offers
# no unprivileged identity.
mkid() {
  rm -f "$1/id"
  cat >"$1/id" <<EOF
#!/usr/bin/env bash
other='${3:-}'
case "\${1:-}" in
  -u|-g)
    if [ -n "\${2:-}" ]; then
      [ -n "\$other" ] || exit 1        # no such account on this box
      echo "\$other"
    else
      echo $2
    fi ;;
  -un|-nu) echo cqlite-test-user ;;
  *)  echo $2 ;;
esac
EOF
  chmod +x "$1/id"
}

# Inert shims for the tools the surrounding bootstrap sections would otherwise
# reach (network / installs). Only the perf-specific shims below are functional.
mkshim() {
  local name="$1"
  cat >"$tmp/$name" <<EOF
#!/usr/bin/env bash
exit 0
EOF
  chmod +x "$tmp/$name"
}
mkshim brew
mkshim cargo
mkshim roborev
mkshim gh
mkuname "$tmp" Linux   # every bootstrap run that includes $tmp in PATH is Linux
mkid "$tmp" 1000       # ...and is a NON-ROOT runner, whatever UID runs the suite
host_home="$tmp/host-home"; mkdir -p "$host_home/.cargo"

# ---- the `perf` shims -------------------------------------------------------------
# The FUNCTIONAL verification must be HONOURED, not merely attempted (the #3119 lesson
# applied to this issue): `perf stat` exits 0 while printing `<not supported>` /
# `<not counted>`, and a virtualised PMU can report a flat 0, so an rc-only check is
# exactly the false green being fixed. Every negative case drives a shim that EXITS 0
# with an unusable counter.
perfbin="$tmp/perfbin"; mkdir -p "$perfbin"
export PERFSHIM_LOG="$tmp/perfshim.log"; : >"$PERFSHIM_LOG"

# mkperfshim <csv-count-field>: an rc-0 perf printing ONE cycles row with that count.
mkperfshim() {
  cat >"$perfbin/perf" <<EOF
#!/usr/bin/env bash
echo "perf \$*" >>"\$PERFSHIM_LOG"
printf '%s\n' '$1,,cycles,100000000,100.00,,'
exit 0
EOF
  chmod +x "$perfbin/perf"
}

# mkperfshim_raw <exit-code> <stdout-line>...: the general shim — an EXACT exit code
# and arbitrary CSV rows (or none). The `perf stat` failure and empty-output paths are
# the REAL states on an unbootstrapped box (paranoid=4 denies the syscall outright, so
# perf exits non-zero), and every shim above exits 0 — so without this those two
# branches were never driven and a mutation making either RETURN SUCCESS survived.
mkperfshim_raw() {
  local rc="$1"; shift
  local data="$perfbin/perf.rows" line
  : >"$data"
  for line in "$@"; do printf '%s\n' "$line" >>"$data"; done
  cat >"$perfbin/perf" <<EOF
#!/usr/bin/env bash
echo "perf \$*" >>"\$PERFSHIM_LOG"
cat "$data"
exit $rc
EOF
  chmod +x "$perfbin/perf"
}

# perf_test_real_dropin_state [path]: the content-AND-metadata identity of the REAL
# managed drop-in, for a BEFORE/AFTER "this run changed nothing" comparison.
#
# WHY NOT "IT DOES NOT EXIST" (issue #3249 review R5-2). The obvious assertion — the real
# /etc/sysctl.d/99-cqlite-perf.conf must not exist — is SELF-DEFEATING: the whole purpose
# of this change is to install that file on the fleet, so the moment a host is legitimately
# bootstrapped the mandatory `tooling-tests` gate component would go red on exactly the
# machines where the feature WORKED. Hermeticity is "this run changed nothing", not "this
# file has never existed", so it is asserted as a before/after comparison of the real path
# (plus the tripwire proving no mutating command was invoked at all).
# `ls -ldn` + the bytes, so a create, a delete, a content change and a mode/owner change
# all show up; no GNU-only `stat -c`. The path is an argument so the comparator itself is
# testable against a file the suite may legitimately write (case 6c).
perf_test_real_dropin_state() {
  local p="${1:-/etc/sysctl.d/99-cqlite-perf.conf}"
  if [ -e "$p" ]; then
    ls -ldn "$p" 2>/dev/null || printf 'unstatable\n'
    cat "$p" 2>/dev/null || printf 'unreadable\n'
  else
    printf 'absent\n'
  fi
}

# The real drop-in's state as it was when this suite STARTED — the baseline every
# host-clean assertion compares against.
perf_real_dropin_before="$tmp/.real-dropin-before"
perf_test_real_dropin_state >"$perf_real_dropin_before" 2>/dev/null

# perf_test_assert_host_clean: nothing in the suite may have CHANGED the real
# /etc/sysctl.d drop-in. Run LAST by every suite that sources this lib — each suite
# asserts it for itself, because "the other file checked it" is not a property either
# file can rely on when they are run independently.
perf_test_assert_host_clean() {
  local after="$tmp/.real-dropin-after"
  perf_test_real_dropin_state >"$after" 2>/dev/null
  if cmp -s "$perf_real_dropin_before" "$after"; then
    ok "perf section: the real /etc/sysctl.d/99-cqlite-perf.conf is byte- and metadata-identical to its pre-suite state (this suite changed nothing)"
  else
    bad "perf section: the suite CHANGED the real /etc/sysctl.d/99-cqlite-perf.conf (before/after differ)"
    diff "$perf_real_dropin_before" "$after" 2>/dev/null | head -6
  fi
}

# perf_test_report: the trailing count line + the suite's exit status.
perf_test_report() {
  echo
  echo "PASS=$PASS FAIL=$FAIL"
  [ "$FAIL" -eq 0 ]
}
