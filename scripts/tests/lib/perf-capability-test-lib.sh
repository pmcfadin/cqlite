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
# ONE SANDBOX ROOT (review R6-1/R6-2). Every seam must now be provably INSIDE the declared
# root CQLITE_PERF_TEST_SANDBOX, which is exported ONCE here as this suite's `$tmp` and
# proves itself with the stamp file the helper looks for. That is why every seam a case sets
# is a path under `$tmp`: containment is the whole check, so anything outside — `//etc`,
# `/tmp/../etc/sysctl.d`, a symlinked ancestor, a relative path — is refused without the
# helper needing to know the name of a single forbidden place.
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
  grep -E '^sudo ' "$1" 2>/dev/null | grep -E '(\btee\b|\bsysctl\b|\btrue\b|\bsh\b)' | grep -v '^sudo -n ' || true
}

# THE STAGED INSTALL IS ONE PRIVILEGED INVOCATION, SO THE TRIPWIRE IS MULTI-LINE (issue #3261,
# roborev round 2). `mktemp` + write + `chmod` + `mv -T` all run inside a single privileged
# `sh -c` — that is the fix for the create->reopen race — so the shim records `sudo -n sh -c `
# followed by the SCRIPT TEXT across many lines. A line-wise `grep 'tee .*99-cqlite-perf.conf'`
# therefore no longer identifies a write, and asserting on it would silently stop testing
# anything. These two helpers name the invocation instead of its internals:
#   perf_write_count <log>   how many privileged staged installs were recorded (expect exactly 1
#                            per write; more than one would mean the consolidation regressed into
#                            several privileged calls, which is the race coming back)
#   perf_wrote_dropin <log>  rc 0 iff a staged install was aimed at the managed drop-in path. The
#                            argv's LAST line carries `perf-capability-install <dir> <path> <base>`,
#                            so this matches the invocation marker AND the target on one line.
perf_write_count() {
  grep -c '^sudo -n sh -c' "$1" 2>/dev/null || true
}
perf_wrote_dropin() {
  grep -q 'perf-capability-install .*99-cqlite-perf\.conf' "$1" 2>/dev/null
}

# `mktemp` IS A COMMAND THAT CAN FAIL, and these suites deliberately run WITHOUT
# `set -e` (issue #3249 review R8-3). An unchecked `tmp=$(mktemp -d …)` that fails —
# full/read-only $TMPDIR, a hostile PATH `mktemp`, a container out of inodes — leaves
# `tmp` EMPTY while every path below is spelled "$tmp/…", so the very next lines would
# write ROOT-LEVEL paths (/global-gitconfig, /perfbin, /perfshim.log, /host-home) and
# the EXIT trap would `rm -rf ""`. These suites run in the MANDATORY `tooling-tests`
# gate component, sometimes under a root identity, so that is host damage caused by a
# test run. Four things must hold — rc 0, non-empty, absolute, and actually a
# directory — or NOTHING happens at all. `exit`, not `return`: this file is sourced by
# a suite with no `set -e`, where a `return 1` would be ignored and execution would
# continue with the empty `tmp` this guard exists to stop.
# Observed firing: case 1x of scripts/tests/test_perf_capability.sh drives both
# failure shapes (non-zero rc, and rc 0 with empty output) and asserts no root-level
# path is created.
if ! tmp=$(mktemp -d "${TMPDIR:-/tmp}/perf-cap-test.XXXXXX"); then tmp=''; fi
case "$tmp" in /*) ;; *) tmp='' ;; esac
if [ -z "$tmp" ] || [ ! -d "$tmp" ]; then
  printf 'perf-capability-test-lib: REFUSING TO RUN (reason: unusable-temp-dir): `mktemp -d` did not yield an existing absolute directory (got %s). Every path in these suites is "$tmp/...", so continuing would write ROOT-LEVEL paths on a box that may be running this suite as root. Check TMPDIR=%s.\n' \
    "'${tmp:-<empty>}'" "'${TMPDIR:-/tmp}'" >&2
  exit 1
fi
trap 'rm -rf "$tmp"' EXIT
# ...and CANONICALIZE it, because this IS the declared sandbox root (issue #3261 AC2). The
# fork-free read gate now rejects a SYMLINKED path component — the fix for a symlink inside the
# sandbox pointing at the real /proc/sys/kernel — and `mktemp -d` legitimately hands back a path
# THROUGH a symlink on some supported hosts (macOS: TMPDIR lives under /var, and /var is a symlink
# to private/var). An uncanonicalized root would make every POSITIVE case refuse there, i.e. the
# suite would be green only on Linux. A root must be spelled as its own destination anyway.
if tmp_canon=$(cd -P -- "$tmp" 2>/dev/null && pwd -P) && [ -n "$tmp_canon" ] && [ -d "$tmp_canon" ]; then
  tmp="$tmp_canon"
fi

# Global-state isolation, same posture as test_bootstrap_agent_machine.sh: the
# bootstrap runs below read/write git config and read board env, and these suites run
# inside `tooling-tests` on the very box hosting a live delivery session.
export GIT_CONFIG_GLOBAL="$tmp/global-gitconfig"
export GIT_CONFIG_NOSYSTEM=1
: >"$GIT_CONFIG_GLOBAL"
unset CQLITE_PROJECT_NUMBER CQLITE_PROJECT_OWNER CQLITE_PROJECT_ACCOUNT PROJECT_TITLE
# A worker shell may export the seams themselves; a test must set exactly what it means.
unset CQLITE_PERF_PROC_DIR CQLITE_PERF_SYSCTL_DIR CQLITE_PERF_SYSCTL_EXTRA_DIRS
# ...and so may a `sudo bash scripts/tests/...` invocation export SUDO_UID/GID/USER,
# which the privilege-drop target resolution reads. A case that means to exercise that
# path sets them itself; inheriting them would make the suite's verdict depend on how
# it was launched (the same env-inheritance class as PERF_SECTION_OK).
unset SUDO_UID SUDO_GID SUDO_USER
# The section under test must never be steered by an ambient export either: bootstrap
# initialises PERF_SECTION_OK itself, and the bootstrap suite proves it.
unset PERF_SECTION_OK
# The path seams are INERT unless this marker is set; under the marker they are MANDATORY,
# must be provably INSIDE the declared sandbox root, and a real sudo/sysctl on PATH is a hard
# refusal (the shim dir is declared per case in CQLITE_PERF_TEST_PRIV_DIR). Cases that must
# exercise the PRODUCTION defaults run with `env -u CQLITE_PERF_TEST_MODE`.
export CQLITE_PERF_TEST_MODE=1
# THE sandbox root for both suites, STAMPED so the helper can PROVE the declaration instead
# of trusting the variable (a bare CQLITE_PERF_TEST_SANDBOX=/etc buys nothing without a stamp
# that only privilege could place there). Every seam any case sets lives under it.
export CQLITE_PERF_TEST_SANDBOX="$tmp"
: >"$tmp/.cqlite-perf-sandbox"

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
