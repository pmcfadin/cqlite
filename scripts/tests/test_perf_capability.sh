#!/usr/bin/env bash
# Regression test for the PERF PROFILING CAPABILITY path (issue #3249):
# scripts/perf-capability.sh (the shared helper) plus the bootstrap section that
# installs and VERIFIES /etc/sysctl.d/99-cqlite-perf.conf.
#
# WHY THIS EXISTS. Agent/worker images ship kernel.perf_event_paranoid = 4 — ALL
# unprivileged perf use denied — and set it in no sysctl file, so a box is
# profileable only by accident and reverts on reboot. Two measurement cycles were
# lost to that, largely because the denial's help text ("access limited") reads
# like a CAPABILITY verdict when it is a PERMISSION verdict.
#
# WHAT IT ASSERTS, beyond "the code is there": that the FUNCTIONAL verification is
# HONOURED. `perf stat` exits 0 while printing `<not supported>`/`<not counted>`,
# and a virtualised PMU can report a flat 0 — so an rc-only check is precisely the
# false green being fixed, and every negative case here drives a shimmed perf that
# EXITS 0 with an unusable counter.
#
# HOST SAFETY. Nothing here touches the real /etc/sysctl.d or /proc: two test-only
# env seams stand in (CQLITE_PERF_PROC_DIR, CQLITE_PERF_SYSCTL_DIR) and every
# privileged/mutating tool (sudo, sysctl, tee, the package managers) is a recording
# PATH shim. The final case asserts that mutation-freedom directly.
#
# Run standalone:   bash scripts/tests/test_perf_capability.sh
# Or via the gate:  scripts/agent-gate.sh runs it in the `tooling-tests` component.
set -uo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
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
# Reference file for the "did WE create the real /etc/sysctl.d drop-in?" attribution
# in the final case: a plain `-nt` comparison against a file created NOW, so no
# `stat -c %Y` (GNU-only) and no wall-clock arithmetic anywhere in this suite.
suite_ref="$tmp/.suite-start"; : >"$suite_ref"

# Global-state isolation, same posture as test_bootstrap_agent_machine.sh: the
# bootstrap runs below read/write git config and read board env, and this suite runs
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
# initialises PERF_SECTION_OK itself, and this suite proves it (case 4h).
unset PERF_SECTION_OK
# The two path seams are INERT unless this marker is set, and the marker itself
# forbids reaching a real sudo/sysctl (the shim dir is declared per case in
# CQLITE_PERF_TEST_PRIV_DIR). Cases that must exercise the PRODUCTION defaults run
# with `env -u CQLITE_PERF_TEST_MODE`.
export CQLITE_PERF_TEST_MODE=1

# mkuname <dir> <sysname>: a `uname` stub, so the Linux/Darwin branch under test is
# the one selected by the CASE, not by whatever host runs the suite. Without this the
# whole suite was silently Linux-host-only: on a Darwin gate host every bootstrap run
# below would take the "nothing to configure on macos" path and 10 cases would fail.
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
mkuname "$tmp" Linux   # every bootstrap run below that includes $tmp in PATH is Linux
mkid "$tmp" 1000       # ...and is a NON-ROOT runner, whatever UID runs the suite
host_home="$tmp/host-home"; mkdir -p "$host_home/.cargo"

# --- 1. The shared helper: scripts/perf-capability.sh ------------------------------
# Agent images ship kernel.perf_event_paranoid = 4 (all unprivileged perf DENIED)
# and set it in no sysctl file, so a box is profileable only by accident and
# reverts on reboot. Bootstrap now installs /etc/sysctl.d/99-cqlite-perf.conf and
# — the part that matters — VERIFIES the result instead of assuming it.
#
# Cases below cover the helper contract, then the two bootstrap modes, and are
# written so NOTHING touches the host: two test-only env seams stand in for the
# real paths (CQLITE_PERF_PROC_DIR for /proc/sys/kernel, CQLITE_PERF_SYSCTL_DIR for
# /etc/sysctl.d) and every privileged/mutating tool is a recording PATH shim. The
# real /etc/sysctl.d is never opened by this suite.
if bash -n "$PERFLIB" 2>/dev/null; then
  ok "perf-capability.sh parses (bash -n)"
else
  bad "perf-capability.sh has a syntax error"
fi

# 1a. Sourcing must have NO side effects: no output, no `set` flag changes, no
#      exit — the gate sources this file inside a summary emit, where any of those
#      would corrupt an unrelated run.
src_probe=$(bash -c '
  set +u +o pipefail
  before=$-
  out=$(. "$1" 2>&1)
  after=$-
  [ -z "$out" ] || echo "OUTPUT:$out"
  [ "$before" = "$after" ] || echo "FLAGS:$before->$after"
  echo DONE' _ "$PERFLIB" 2>&1)
if [ "$src_probe" = DONE ]; then
  ok "perf-capability: sourcing is side-effect free (no output, no set-flag change)"
else
  bad "perf-capability: sourcing had a side effect: $src_probe"
fi

# 1b. The FREE token read: every state comes from /proc alone, and a bad value is
#      reported as unknown rather than GUESSED (no-heuristics, #28).
perfproc="$tmp/perfproc"; mkdir -p "$perfproc"
token_for() { # token_for <paranoid> <kptr>
  printf '%s\n' "$1" >"$perfproc/perf_event_paranoid"
  printf '%s\n' "$2" >"$perfproc/kptr_restrict"
  CQLITE_PERF_PROC_DIR="$perfproc" bash "$PERFLIB" --token
}
tok_fail=0
check_token() { # check_token <paranoid> <kptr> <expected>
  local got; got=$(token_for "$1" "$2")
  [ "$got" = "$3" ] || { bad "perf-capability: paranoid=$1 kptr=$2 -> '$got' (expected '$3')"; tok_fail=1; }
}
check_token -1 0 ok
check_token 0 0 ok
check_token 1 0 paranoid-1
check_token 4 1 paranoid-4
check_token 2 0 paranoid-2
check_token -1 1 kptr-restricted
check_token 0 2 kptr-restricted
check_token garbage 0 unknown
# A malformed or oversized value must NOT slip past the `>= 1` comparison and be
# reported as `ok`: `[ 1abc -ge 1 ]` / `[ 99999999999999999999999 -ge 1 ]` do not
# compare — they print "integer expression expected" and return FALSE.
check_token '1abc' 0 unknown
check_token 99999999999999999999999 0 unknown
check_token -1 '0x0' unknown
# ...and that rejection must be SILENT: this runs inside the gate's summary emit,
# where a stray stderr line lands in the gate's own output.
noise=$(printf '1abc\n' >"$perfproc/perf_event_paranoid"; printf '0\n' >"$perfproc/kptr_restrict"
  CQLITE_PERF_PROC_DIR="$perfproc" bash "$PERFLIB" --token 2>&1 >/dev/null)
if [ -z "$noise" ]; then
  ok "perf-capability: a malformed /proc value is rejected SILENTLY (no stderr noise)"
else
  bad "perf-capability: malformed value leaked to stderr: $noise"
fi
if [ "$tok_fail" -eq 0 ]; then
  ok "perf-capability: token reflects /proc exactly (ok / paranoid-N / kptr-restricted / unknown)"
fi
if [ "$(CQLITE_PERF_PROC_DIR="$tmp/no-such-proc" bash "$PERFLIB" --token)" = absent ]; then
  ok "perf-capability: missing /proc controls report 'absent' (container), never a guess"
else
  bad "perf-capability: missing /proc controls did not report 'absent'"
fi
# The token read must be MUTATION-FREE: it is what the gate calls on every run.
before_hash=$(cat "$perfproc"/* | cksum)
CQLITE_PERF_PROC_DIR="$perfproc" CQLITE_PERF_SYSCTL_DIR="$tmp/perf-sysctl-untouched" \
  bash "$PERFLIB" --token >/dev/null 2>&1
if [ "$(cat "$perfproc"/* | cksum)" = "$before_hash" ] && [ ! -d "$tmp/perf-sysctl-untouched" ]; then
  ok "perf-capability: the token read mutates nothing (no /proc write, no sysctl.d dir)"
else
  bad "perf-capability: the token read mutated state"
fi

# 1c. The drop-in bytes carry the -1-not-1 rationale and BOTH controls, and the
#      printed remedy (`--drop-in | sudo tee …`) is what produces them — so a
#      hand-applied fix is byte-identical and the next bootstrap run is a no-op.
dropin=$(bash "$PERFLIB" --drop-in)
if printf '%s\n' "$dropin" | grep -q '^kernel.perf_event_paranoid = -1$' \
   && printf '%s\n' "$dropin" | grep -q '^kernel.kptr_restrict = 0$' \
   && printf '%s\n' "$dropin" | grep -qi 'cumulative' \
   && printf '%s\n' "$dropin" | grep -qi 'multi-tenant'; then
  ok "perf-capability: drop-in sets both controls and states the rationale + posture"
else
  bad "perf-capability: drop-in content is missing a control, the rationale or the posture"
  printf '%s\n' "$dropin"
fi
# 1c-i. The PRODUCTION defaults, asserted with the seams OFF. Every other case here
#       sets both seams, so a default changed to /tmp/bogus-* would have gone
#       unnoticed; these two read-only string asserts pin the real literals. They
#       stay hermetic — nothing is read or written, only the resolved path printed.
if [ "$(env -u CQLITE_PERF_TEST_MODE -u CQLITE_PERF_SYSCTL_DIR -u CQLITE_PERF_PROC_DIR \
          bash "$PERFLIB" --drop-in-path)" = /etc/sysctl.d/99-cqlite-perf.conf ]; then
  ok "perf-capability: the DEFAULT drop-in path is /etc/sysctl.d/99-cqlite-perf.conf (survives reboot)"
else
  bad "perf-capability: unexpected default drop-in path"
fi
default_proc=$(env -u CQLITE_PERF_TEST_MODE -u CQLITE_PERF_SYSCTL_DIR -u CQLITE_PERF_PROC_DIR \
  bash -c '. "$1"; perf_capability_proc_dir' _ "$PERFLIB")
if [ "$default_proc" = /proc/sys/kernel ]; then
  ok "perf-capability: the DEFAULT proc dir is /proc/sys/kernel"
else
  bad "perf-capability: unexpected default proc dir: '$default_proc'"
fi

# 1c-ii. The test seams are INERT without the marker, and a PRIVILEGED caller REFUSES
#        outright. This is the security property: bootstrap pipes the drop-in through
#        `sudo tee <path>`, so an env-derived destination let one stray export
#        (CQLITE_PERF_SYSCTL_DIR=/etc/sudoers.d) make ROOT write an env-chosen file
#        while the real drop-in was never installed — and an unparsable sudoers entry
#        can wedge `sudo` outright. Same for a fake /proc fabricating a verdict.
seam_no_marker_path=$(env -u CQLITE_PERF_TEST_MODE CQLITE_PERF_SYSCTL_DIR="$tmp/evil-sysctl.d" \
  bash "$PERFLIB" --drop-in-path)
if [ "$seam_no_marker_path" = /etc/sysctl.d/99-cqlite-perf.conf ]; then
  ok "perf-capability: CQLITE_PERF_SYSCTL_DIR is INERT without CQLITE_PERF_TEST_MODE=1 (path stays the hardcoded literal)"
else
  bad "perf-capability: a seam without the marker steered the drop-in path to '$seam_no_marker_path'"
fi
seam_no_marker_proc=$(env -u CQLITE_PERF_TEST_MODE CQLITE_PERF_PROC_DIR="$perfproc" \
  bash -c '. "$1"; perf_capability_proc_dir' _ "$PERFLIB")
if [ "$seam_no_marker_proc" = /proc/sys/kernel ]; then
  ok "perf-capability: CQLITE_PERF_PROC_DIR is INERT without the marker (no fabricated /proc verdict)"
else
  bad "perf-capability: a seam without the marker steered the /proc read to '$seam_no_marker_proc'"
fi
guard_out=$(env -u CQLITE_PERF_TEST_MODE CQLITE_PERF_SYSCTL_DIR="$tmp/evil-sysctl.d" \
  bash -c '. "$1"; perf_capability_env_guard' _ "$PERFLIB" 2>&1); guard_rc=$?
if [ "$guard_rc" -ne 0 ] && printf '%s' "$guard_out" | grep -qi 'REFUSING'; then
  ok "perf-capability: env guard REFUSES loudly when a seam is set without the marker"
else
  bad "perf-capability: env guard allowed a marker-less seam (rc=$guard_rc, out='$guard_out')"
fi
# ...and the marker is itself hermetic: with it set, a REAL sudo/sysctl on PATH is a
# refusal, so test mode can never reach a real privileged tool.
realpriv="$tmp/realpriv"; mkdir -p "$realpriv"
for t in sudo sysctl; do
  printf '#!/usr/bin/env bash\nexit 0\n' >"$realpriv/$t"; chmod +x "$realpriv/$t"
done
guard2_out=$(PATH="$realpriv:$PATH" CQLITE_PERF_TEST_MODE=1 CQLITE_PERF_TEST_PRIV_DIR="$tmp/some-other-dir" \
  bash -c '. "$1"; perf_capability_env_guard' _ "$PERFLIB" 2>&1); guard2_rc=$?
if [ "$guard2_rc" -ne 0 ] && printf '%s' "$guard2_out" | grep -q 'outside the declared shim dir'; then
  ok "perf-capability: test mode REFUSES when sudo resolves outside the declared shim dir"
else
  bad "perf-capability: test mode accepted an undeclared sudo (rc=$guard2_rc, out='$guard2_out')"
fi
# ...and `sysctl` is guarded as strictly as `sudo` (a real `sysctl --system` would
# reconfigure the HOST kernel, marker or not).
guard3_out=$(PATH="$realpriv:$PATH" CQLITE_PERF_TEST_MODE=1 CQLITE_PERF_TEST_PRIV_DIR="$tmp/only-sudo-here" \
  bash -c '. "$1"; perf_capability_env_guard' _ "$PERFLIB" 2>&1)
if printf '%s' "$guard3_out" | grep -q 'sysctl resolves to\|sudo resolves to'; then
  ok "perf-capability: test mode guards BOTH sudo and sysctl against a real binary"
else
  bad "perf-capability: test mode did not name the offending privileged tool: '$guard3_out'"
fi
if PATH="$realpriv:$PATH" CQLITE_PERF_TEST_MODE=1 CQLITE_PERF_TEST_PRIV_DIR="$realpriv" \
     bash -c '. "$1"; perf_capability_env_guard' _ "$PERFLIB" 2>/dev/null; then
  ok "perf-capability: test mode ACCEPTS a sudo inside the declared shim dir"
else
  bad "perf-capability: test mode rejected its own declared shim dir"
fi

# 1d. The FUNCTIONAL verification is HONOURED, not merely attempted — the #3119
#      lesson (test_bootstrap_agent_machine.sh case 10) applied to this issue. `perf stat` exits 0 while
#      printing `<not supported>` / `<not counted>`, and a virtualised PMU can
#      report a flat 0, so an rc-only check is exactly the false green being fixed.
perfbin="$tmp/perfbin"; mkdir -p "$perfbin"
mkperfshim() { # mkperfshim <csv-count-field>
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
export PERFSHIM_LOG="$tmp/perfshim.log"; : >"$PERFSHIM_LOG"
verify_verdict() { # verify_verdict <csv-count-field> -> "<rc> <stdout>"
  mkperfshim "$1"
  local out rc
  out=$(PATH="$perfbin:$PATH" bash "$PERFLIB" --verify 2>&1); rc=$?
  printf '%s %s' "$rc" "$out"
}
ver_fail=0
case "$(verify_verdict 4242424)" in
  "0 cycles=4242424") ;;
  *) bad "perf-capability: a real non-zero cycle count was not accepted: $(verify_verdict 4242424)"; ver_fail=1 ;;
esac
for badcount in 0 '<not supported>' '<not counted>' 'nonsense'; do
  v=$(verify_verdict "$badcount")
  case "$v" in
    0\ *) bad "perf-capability: rc-0 perf with count '$badcount' was accepted as verified ($v)"; ver_fail=1 ;;
  esac
done
if [ "$ver_fail" -eq 0 ]; then
  ok "perf-capability: verify requires a NON-ZERO cycle count (0 / <not supported> / <not counted> all FAIL)"
fi
# 1d-i. HYBRID PMU (Intel 12th-gen+ P/E cores): perf reports one row per PMU with
#       QUALIFIED event names (`cpu_core/cycles/`, `cpu_atom/cycles/`), routinely with
#       `<not supported>` on the sibling that did not run. A parser keyed on a literal
#       leading `cycles` calls that good collection `no-cycles-row`, i.e. reports a
#       profileable box as broken. Accept the qualified name and the positive row.
hybrid_v=$(mkperfshim_raw 0 '<not supported>,,cpu_atom/cycles/,0,100.00,,' '31415926,,cpu_core/cycles/,100000000,100.00,,'
  PATH="$perfbin:$PATH" bash "$PERFLIB" --verify 2>&1)
if [ "$hybrid_v" = "cycles=31415926" ]; then
  ok "perf-capability: a hybrid-PMU qualified cycle row (cpu_core/cycles/) is accepted, sibling <not supported> ignored"
else
  bad "perf-capability: hybrid-PMU rows misparsed: '$hybrid_v'"
fi
# ...and the order must not matter (positive row first, unsupported sibling second).
hybrid2_v=$(mkperfshim_raw 0 '2718281,,cpu_core/cycles/,100000000,100.00,,' '<not supported>,,cpu_atom/cycles/,0,100.00,,'
  PATH="$perfbin:$PATH" bash "$PERFLIB" --verify 2>&1)
if [ "$hybrid2_v" = "cycles=2718281" ]; then
  ok "perf-capability: a hybrid-PMU positive row is accepted regardless of row order"
else
  bad "perf-capability: hybrid-PMU row order changed the verdict: '$hybrid2_v'"
fi
# ...while a hybrid box where NO PMU counted is still a failure, not a pass.
hybrid3_v=$(mkperfshim_raw 0 '<not supported>,,cpu_atom/cycles/,0,100.00,,' '<not counted>,,cpu_core/cycles/,0,100.00,,'
  PATH="$perfbin:$PATH" bash "$PERFLIB" --verify 2>&1); hybrid3_rc=$?
if [ "$hybrid3_rc" -ne 0 ] && printf '%s' "$hybrid3_v" | grep -q 'counter-not-supported'; then
  ok "perf-capability: a hybrid box where NO PMU counted still FAILS (counter-not-supported)"
else
  bad "perf-capability: an all-unsupported hybrid collection was accepted (rc=$hybrid3_rc, '$hybrid3_v')"
fi

# 1d-ii. `perf stat` EXITING NON-ZERO is the actual paranoid=4 state — the denial this
#        whole issue is about — and every shim above exits 0, so the branch shipped
#        untested: a mutation making it print `cycles=1` and return 0 survived. Drive
#        it with the real help text a denied perf prints.
mkperfshim_raw 1 'Error:' 'Access to performance monitoring and observability operations is limited.' \
  'Consider adjusting /proc/sys/kernel/perf_event_paranoid setting to open' >/dev/null
denied_v=$(PATH="$perfbin:$PATH" bash "$PERFLIB" --verify 2>&1); denied_rc=$?
if [ "$denied_rc" -ne 0 ] && printf '%s' "$denied_v" | grep -q '^perf-stat-failed rc=1' \
   && printf '%s' "$denied_v" | grep -qi 'observability operations is limited'; then
  ok "perf-capability: a DENIED perf (non-zero exit, 'access limited') fails with perf-stat-failed rc=1 + the text"
else
  bad "perf-capability: a non-zero perf exit was not surfaced (rc=$denied_rc, '$denied_v')"
fi
# 1d-iii. rc 0 with NO output at all: a masked/absent PMU. Must be no-cycles-row, not
#         a pass (that branch was equally untested).
empty_v=$(mkperfshim_raw 0 >/dev/null; PATH="$perfbin:$PATH" bash "$PERFLIB" --verify 2>&1); empty_rc=$?
if [ "$empty_rc" -ne 0 ] && printf '%s' "$empty_v" | grep -q 'no-cycles-row'; then
  ok "perf-capability: rc-0 perf with EMPTY output fails with no-cycles-row"
else
  bad "perf-capability: rc-0 perf with empty output was accepted (rc=$empty_rc, '$empty_v')"
fi
# 1d-iv. An OVERSIZED/malformed count must fail CLOSED and SILENTLY: `[ 999…9 -le 0 ]`
#        returns 2 (neither true nor false), so an unvalidated operand fell through to
#        the VERIFIED return while leaking "integer expression expected" to stderr.
mkperfshim 99999999999999999999999
big_out=$(PATH="$perfbin:$PATH" bash "$PERFLIB" --verify 2>/dev/null); big_rc=$?
big_err=$(PATH="$perfbin:$PATH" bash "$PERFLIB" --verify 2>&1 >/dev/null)
if [ "$big_rc" -ne 0 ] && printf '%s' "$big_out" | grep -q 'unparseable-count=' && [ -z "$big_err" ]; then
  ok "perf-capability: an oversized cycle count fails CLOSED as unparseable-count, with no stderr leak"
else
  bad "perf-capability: oversized count mishandled (rc=$big_rc, out='$big_out', err='$big_err')"
fi
mkperfshim '12x'
mal_out=$(PATH="$perfbin:$PATH" bash "$PERFLIB" --verify 2>/dev/null); mal_rc=$?
mal_err=$(PATH="$perfbin:$PATH" bash "$PERFLIB" --verify 2>&1 >/dev/null)
if [ "$mal_rc" -ne 0 ] && printf '%s' "$mal_out" | grep -q 'unparseable-count=12x' && [ -z "$mal_err" ]; then
  ok "perf-capability: a malformed cycle count fails CLOSED as unparseable-count, with no stderr leak"
else
  bad "perf-capability: malformed count mishandled (rc=$mal_rc, out='$mal_out', err='$mal_err')"
fi
# 1d-v. The idempotency byte-compare must not depend on `diff`: without diffutils
#       `diff -q` exits 127, which reads as "different" — so every --yes run re-wrote
#       the file AND then falsely reported it could not write it.
nodiff="$tmp/nodiff-bin"; mkdir -p "$nodiff"
for t in bash cat; do s=$(command -v "$t" 2>/dev/null) && ln -sf "$s" "$nodiff/$t"; done
nodiff_dir="$tmp/nodiff-sysctl.d"; mkdir -p "$nodiff_dir"
bash "$PERFLIB" --drop-in >"$nodiff_dir/99-cqlite-perf.conf"
if PATH="$nodiff" CQLITE_PERF_TEST_MODE=1 CQLITE_PERF_SYSCTL_DIR="$nodiff_dir" \
     bash -c '. "$1"; perf_capability_dropin_current' _ "$PERFLIB" 2>/dev/null; then
  ok "perf-capability: the idempotency compare works with NO 'diff' binary on PATH"
else
  bad "perf-capability: the idempotency compare needs diffutils (a box without it re-writes forever)"
fi

# ...and it must actually run the per-CPU collection the doctrine mandates.
if grep -q 'stat .*-C 0' "$PERFSHIM_LOG" && grep -q '\-e cycles' "$PERFSHIM_LOG"; then
  ok "perf-capability: verify runs 'perf stat -C 0 -e cycles' (per-CPU, as doctrine requires)"
else
  bad "perf-capability: verify did not run a per-CPU 'perf stat -C 0 -e cycles'"
  cat "$PERFSHIM_LOG"
fi
# No perf binary at all is a warn-worthy UNVERIFIED, never a silent pass.
noperf_dir="$tmp/noperf"; mkdir -p "$noperf_dir"
for t in bash cat awk printf tr cut command timeout; do
  s=$(command -v "$t" 2>/dev/null) && ln -sf "$s" "$noperf_dir/$t"
done
if noperf_out=$(PATH="$noperf_dir" bash "$PERFLIB" --verify 2>&1); then
  bad "perf-capability: verify PASSED with no perf binary on PATH"
else
  case "$noperf_out" in
    *no-perf-binary*) ok "perf-capability: verify fails with 'no-perf-binary' when perf is absent" ;;
    *) bad "perf-capability: unexpected no-perf verdict: $noperf_out" ;;
  esac
fi

# --- 2. Bootstrap perf section: the DEFAULT (no --yes) run mutates NOTHING ----
# Tripwire shims for every privileged/mutating tool the write path could use, so
# "wrote nothing" is PROVEN rather than assumed. `perf` may be invoked (the
# verification is read-only and is the point of the section); `sudo`, `sysctl` and
# `tee` may not.
perfshims="$tmp/perf-shims"; mkdir -p "$perfshims"
perftrip="$tmp/perf-tripwire.log"; : >"$perftrip"
for t in sudo sysctl tee; do
  cat >"$perfshims/$t" <<EOF
#!/usr/bin/env bash
echo "$t \$*" >>"$perftrip"
exit 0
EOF
  chmod +x "$perfshims/$t"
done
mkperfroot() { # mkperfroot <dir>: a throwaway REPO_ROOT bootstrap can resolve
  local dir="$1"
  mkdir -p "$dir/scripts/lib"
  cp "$BOOTSTRAP" "$dir/scripts/bootstrap-agent-machine.sh"
  cp "$PERFLIB" "$dir/scripts/perf-capability.sh"
  cp "$SCRIPT_DIR/../lib/gate-notify.sh" "$dir/scripts/lib/gate-notify.sh" 2>/dev/null || true
}
perf_sysctl_d="$tmp/perf-sysctl.d"; mkdir -p "$perf_sysctl_d"
perf_proc="$tmp/perf-proc"; mkdir -p "$perf_proc"
printf '4\n' >"$perf_proc/perf_event_paranoid"
printf '1\n' >"$perf_proc/kptr_restrict"
mkperfroot "$tmp/perf-root-check"
mkperfshim 999999
check_out=$(PATH="$perfshims:$perfbin:$tmp:$PATH" HOME="$host_home" CARGO_HOME="$host_home/.cargo" \
  CQLITE_PERF_PROC_DIR="$perf_proc" CQLITE_PERF_SYSCTL_DIR="$perf_sysctl_d" CQLITE_PERF_TEST_PRIV_DIR="$perfshims" \
  bash "$tmp/perf-root-check/scripts/bootstrap-agent-machine.sh" --skip-smoke 2>&1)
check_rc=$?
if printf '%s' "$check_out" | grep -q 'Perf profiling capability'; then
  ok "perf section: present in the bootstrap run"
else
  bad "perf section: MISSING from the bootstrap run"
fi
# The ONLY tolerated privileged invocation is the non-mutating `sudo -n true`
# availability probe (bootstrap must know whether to print a sudo remedy at all);
# anything else — a `tee`, a `sysctl`, or any other sudo command — is a mutation.
perf_mutating=$(grep -vE '^sudo -n true$' "$perftrip" | grep -E '^(sudo|sysctl|tee) ' | head -5)
if [ "$check_rc" -eq 0 ] && [ -z "$(ls -A "$perf_sysctl_d")" ] && [ -z "$perf_mutating" ]; then
  ok "perf section: default (no --yes) run wrote NO drop-in and invoked no privileged/mutating command"
else
  bad "perf section: default run mutated state (rc=$check_rc, dir='$(ls -A "$perf_sysctl_d")', tripwire below)"
  cat "$perftrip"
fi
# Whatever it DID invoke must be non-interactive: every recorded sudo line begins
# `sudo -n `, so no code path can sit on a password prompt on an unattended worker.
check_bare_sudo=$(sudo_perf_offenders "$perftrip")
if grep -q '^sudo -n true$' "$perftrip" && [ -z "$check_bare_sudo" ]; then
  ok "perf section: the default run's sudo probe carries -n (never interactive)"
else
  bad "perf section: a sudo invocation without -n was recorded (or the probe never ran): $(cat "$perftrip")"
fi
# It must instead PRINT the exact remedy, and the remedy must WRITE **AND APPLY**: a
# write-only line leaves the operator with a reboot-persistent file and an
# unprofileable box until reboot — the very failure the functional verification
# exists to prevent, on the path most people take (no --yes). Plus the AC5 posture
# and the BPF caveat (#3217).
if printf '%s' "$check_out" | grep -q 'perf-capability.sh --drop-in | sudo tee .*99-cqlite-perf.conf >/dev/null && sudo sysctl -q --system' \
   && printf '%s' "$check_out" | grep -q 're-run with --yes'; then
  ok "perf section: prints the complete WRITE-AND-APPLY remedy line instead of running it"
else
  bad "perf section: the printed remedy is missing the apply (or absent)"
  printf '%s\n' "$check_out" | sed -n '/Perf profiling/,/^$/p'
fi
# The PRE-state line is asserted SPECIFICALLY (not just "a line somewhere mentions
# paranoid-4"): it is the diagnosis an operator reads first, and a later warn line
# carrying the same token would otherwise satisfy a loose grep.
if printf '%s\n' "$check_out" | grep -q 'runtime now: perf_event_paranoid=4 kptr_restrict=1 .*gate stamps perf=paranoid-4'; then
  ok "perf section: the pre-state line reports the ACTUAL /proc values and the token the gate will stamp"
else
  bad "perf section: the pre-state line is wrong or missing"
  printf '%s\n' "$check_out" | grep 'runtime now' || true
fi
if printf '%s' "$check_out" | grep -qi 'BPF collectors .* require sudo' \
   && printf '%s' "$check_out" | grep -qi 'never apply it to a shared or multi-tenant host'; then
  ok "perf section: states the BPF-still-needs-sudo caveat and the single-tenant posture"
else
  bad "perf section: missing the BPF caveat or the security posture"
fi
# A blocked box must be DIAGNOSED as a permission verdict, not a capability one.
if printf '%s' "$check_out" | grep -q 'perf=paranoid-4' \
   && printf '%s' "$check_out" | grep -qi 'PERMISSION verdict'; then
  ok "perf section: paranoid-4 is reported as a PERMISSION verdict with the gate's token"
else
  bad "perf section: a blocked box was not diagnosed as a permission verdict"
  printf '%s\n' "$check_out" | sed -n '/Perf profiling/,/^$/p'
fi

# --- 3. Bootstrap perf section under --yes: write -> READ BACK -> verify ------
# The privileged shims here are FUNCTIONAL, not inert: `sudo` records and execs,
# `sysctl --system` records and (like the kernel would) updates the fake procfs, so
# the read-back has something real to read. Order is asserted, because a read-back
# that runs BEFORE the apply proves nothing.
yesshims="$tmp/perf-yes-shims"; mkdir -p "$yesshims"
yestrip="$tmp/perf-yes-tripwire.log"; : >"$yestrip"
cat >"$yesshims/sudo" <<EOF
#!/usr/bin/env bash
echo "sudo \$*" >>"$yestrip"
while [ "\${1:-}" = "-n" ]; do shift; done
exec "\$@"
EOF
cat >"$yesshims/sysctl" <<EOF
#!/usr/bin/env bash
echo "sysctl \$*" >>"$yestrip"
case " \$* " in *" --system "*)
  printf -- '-1\n' >"$tmp/perf-proc-yes/perf_event_paranoid"
  printf '0\n'     >"$tmp/perf-proc-yes/kptr_restrict" ;;
esac
exit 0
EOF
# apt-get/apt: --yes would otherwise reach the real package manager via the mold
# section. Record-only, so this suite can never install anything on the host.
for t in apt-get apt dnf yum pacman; do
  cat >"$yesshims/$t" <<EOF
#!/usr/bin/env bash
echo "$t \$*" >>"$yestrip"
exit 0
EOF
done
chmod +x "$yesshims"/*
proc_yes="$tmp/perf-proc-yes"; mkdir -p "$proc_yes"
printf '4\n' >"$proc_yes/perf_event_paranoid"
printf '1\n' >"$proc_yes/kptr_restrict"
sysctl_yes="$tmp/perf-sysctl-yes.d"; mkdir -p "$sysctl_yes"
mkperfroot "$tmp/perf-root-yes"
mkperfshim 7777777
yes_home="$tmp/perf-yes-home"; mkdir -p "$yes_home/.cargo"
yes_out=$(PATH="$yesshims:$perfbin:$tmp:$PATH" HOME="$yes_home" CARGO_HOME="$yes_home/.cargo" \
  CQLITE_PERF_PROC_DIR="$proc_yes" CQLITE_PERF_SYSCTL_DIR="$sysctl_yes" CQLITE_PERF_TEST_PRIV_DIR="$yesshims" \
  bash "$tmp/perf-root-yes/scripts/bootstrap-agent-machine.sh" --skip-smoke --yes 2>&1)
yes_rc=$?
if [ "$yes_rc" -eq 0 ]; then
  ok "perf section: --yes run exits 0"
else
  bad "perf section: --yes run exited non-zero (rc=$yes_rc)"
fi
if [ -f "$sysctl_yes/99-cqlite-perf.conf" ] \
   && diff -q <(bash "$PERFLIB" --drop-in) "$sysctl_yes/99-cqlite-perf.conf" >/dev/null 2>&1; then
  ok "perf section: --yes wrote the drop-in with EXACTLY the canonical bytes"
else
  bad "perf section: --yes did not write the canonical drop-in"
  ls -l "$sysctl_yes" 2>&1 | head -3
fi
if grep -q 'tee .*99-cqlite-perf.conf' "$yestrip" && grep -q 'sysctl -q --system' "$yestrip"; then
  ok "perf section: --yes wrote through sudo tee and applied with 'sysctl --system'"
else
  bad "perf section: --yes did not use sudo tee + sysctl --system"
  cat "$yestrip"
fi
# ...and EVERY privileged invocation carried `-n`: an unattended worker must never be
# able to sit on a password prompt, so `PERF_ROOT=(sudo)` (no -n) is a defect even
# though every functional assert above would still pass.
yes_bare_sudo=$(sudo_perf_offenders "$yestrip")
if [ -z "$yes_bare_sudo" ] && grep -q '^sudo -n tee ' "$yestrip" && grep -q '^sudo -n sysctl ' "$yestrip"; then
  ok "perf section: every --yes privileged call went through 'sudo -n' (write AND apply, never interactive)"
else
  bad "perf section: a privileged call did not carry sudo -n: $(cat "$yestrip")"
fi
# ORDER: the apply must precede the read-back verdict, and the functional verify
# must come last (it is the verdict on the whole section).
yes_perf_block=$(printf '%s\n' "$yes_out" | sed -n '/Perf profiling capability/,/^$/p')
n_wrote=$(printf '%s\n' "$yes_perf_block" | grep -n 'wrote .*99-cqlite-perf.conf' | head -1 | cut -d: -f1)
n_read=$(printf '%s\n' "$yes_perf_block" | grep -n 'READ BACK from /proc' | head -1 | cut -d: -f1)
n_verify=$(printf '%s\n' "$yes_perf_block" | grep -n 'perf capability VERIFIED' | head -1 | cut -d: -f1)
if [ -n "$n_wrote" ] && [ -n "$n_read" ] && [ -n "$n_verify" ] \
   && [ "$n_wrote" -lt "$n_read" ] && [ "$n_read" -lt "$n_verify" ]; then
  ok "perf section: order is write -> read-back from /proc -> functional verify"
else
  bad "perf section: wrong order (wrote=$n_wrote read-back=$n_read verify=$n_verify)"
  printf '%s\n' "$yes_perf_block"
fi
# Idempotency: a SECOND --yes run must write nothing (byte-compare no-op).
: >"$yestrip"
yes2_out=$(PATH="$yesshims:$perfbin:$tmp:$PATH" HOME="$yes_home" CARGO_HOME="$yes_home/.cargo" \
  CQLITE_PERF_PROC_DIR="$proc_yes" CQLITE_PERF_SYSCTL_DIR="$sysctl_yes" CQLITE_PERF_TEST_PRIV_DIR="$yesshims" \
  bash "$tmp/perf-root-yes/scripts/bootstrap-agent-machine.sh" --skip-smoke --yes 2>&1)
if printf '%s' "$yes2_out" | grep -q 'drop-in already current' \
   && ! grep -q 'tee .*99-cqlite-perf.conf' "$yestrip"; then
  ok "perf section: a second --yes run is an idempotent no-op (no re-write)"
else
  bad "perf section: second --yes run re-wrote the drop-in"
  printf '%s\n' "$yes2_out" | sed -n '/Perf profiling/,/^$/p'
fi

# --- 4. The verification VERDICT is honoured, and no-sudo degrades gracefully --
# 4a. A perf that exits 0 but reports an unusable counter must produce a WARN and
#      must NEVER be reported as verified. This is the #3119-style mutation (test_bootstrap_agent_machine.sh case 10) applied
#      to AC2: without it, "the check exists" would pass while the box is unusable.
mkperfshim '<not supported>'
unsup_out=$(PATH="$yesshims:$perfbin:$tmp:$PATH" HOME="$yes_home" CARGO_HOME="$yes_home/.cargo" \
  CQLITE_PERF_PROC_DIR="$proc_yes" CQLITE_PERF_SYSCTL_DIR="$sysctl_yes" CQLITE_PERF_TEST_PRIV_DIR="$yesshims" \
  bash "$tmp/perf-root-yes/scripts/bootstrap-agent-machine.sh" --skip-smoke --yes 2>&1)
unsup_rc=$?
if printf '%s' "$unsup_out" | grep -q 'perf capability NOT verified' \
   && ! printf '%s' "$unsup_out" | grep -q 'perf capability VERIFIED' \
   && [ "$unsup_rc" -eq 0 ]; then
  ok "perf section: an rc-0 perf with an unusable counter WARNs (never 'verified'), run still exits 0"
else
  bad "perf section: unusable counter was not surfaced (rc=$unsup_rc)"
  printf '%s\n' "$unsup_out" | sed -n '/Perf profiling/,/^$/p'
fi
# 4b. Zero cycles is the virtualised-PMU case — same verdict.
mkperfshim 0
zero_out=$(PATH="$yesshims:$perfbin:$tmp:$PATH" HOME="$yes_home" CARGO_HOME="$yes_home/.cargo" \
  CQLITE_PERF_PROC_DIR="$proc_yes" CQLITE_PERF_SYSCTL_DIR="$sysctl_yes" CQLITE_PERF_TEST_PRIV_DIR="$yesshims" \
  bash "$tmp/perf-root-yes/scripts/bootstrap-agent-machine.sh" --skip-smoke --yes 2>&1)
if printf '%s' "$zero_out" | grep -q 'perf capability NOT verified' \
   && ! printf '%s' "$zero_out" | grep -q 'perf capability VERIFIED'; then
  ok "perf section: a zero cycle count WARNs (never 'verified')"
else
  bad "perf section: a zero cycle count was accepted as verified"
  printf '%s\n' "$zero_out" | sed -n '/Perf profiling/,/^$/p'
fi
# 4c. A box with NO sudo BINARY: warn + a remedy that actually works there, no write,
#      still exit 0. The generic `... | sudo tee` line is USELESS on this box, so the
#      case asserts the root-shell remedy AND the absence of the sudo one — the two
#      `sudo -n` failure modes (no binary vs needs a password) are different boxes.
nosudo="$tmp/perf-nosudo"; mkdir -p "$nosudo"
for t in bash cat sed awk grep printf tr cut sort head tail wc env date mktemp \
         diff timeout dirname basename ls rm mv cp mkdir touch git python3 hostname stat find; do
  s=$(command -v "$t" 2>/dev/null) && ln -sf "$s" "$nosudo/$t"
done
mkuname "$nosudo" Linux
mkid "$nosudo" 1000    # non-root, whatever UID runs the suite
ln -sf "$perfbin/perf" "$nosudo/perf"
mkperfshim 5555555
nosudo_sysctl="$tmp/perf-nosudo.d"; mkdir -p "$nosudo_sysctl"
nosudo_out=$(PATH="$nosudo" HOME="$yes_home" CARGO_HOME="$yes_home/.cargo" \
  CQLITE_PERF_PROC_DIR="$proc_yes" CQLITE_PERF_SYSCTL_DIR="$nosudo_sysctl" CQLITE_PERF_TEST_PRIV_DIR="$nosudo" \
  bash "$tmp/perf-root-yes/scripts/bootstrap-agent-machine.sh" --skip-smoke --yes 2>&1)
nosudo_rc=$?
if [ "$nosudo_rc" -eq 0 ] && [ -z "$(ls -A "$nosudo_sysctl")" ] \
   && printf '%s' "$nosudo_out" | grep -q "no 'sudo' binary on this box" \
   && printf '%s' "$nosudo_out" | grep -q 'ROOT shell:.*--drop-in > .*99-cqlite-perf.conf && sysctl -q --system' \
   && ! printf '%s' "$nosudo_out" | grep -q 'sudo tee .*99-cqlite-perf.conf'; then
  ok "perf section: a box with no sudo BINARY warns + prints the root-shell remedy (never a useless 'sudo tee'), writes nothing, exits 0"
else
  bad "perf section: no-sudo box mishandled (rc=$nosudo_rc, dir='$(ls -A "$nosudo_sysctl")')"
  printf '%s\n' "$nosudo_out" | sed -n '/Perf profiling/,/^$/p'
fi

# 4c-ii. A box WITH sudo whose `sudo -n` fails (a password is required) is a DIFFERENT
#        box: the `sudo tee` line does work there, interactively. Bootstrap must say so
#        and must still never prompt.
pwsudo="$tmp/perf-pwsudo"; mkdir -p "$pwsudo"
for t in bash cat sed awk grep printf tr cut sort head tail wc env date mktemp \
         diff timeout dirname basename ls rm mv cp mkdir touch git python3 hostname stat find; do
  s=$(command -v "$t" 2>/dev/null) && ln -sf "$s" "$pwsudo/$t"
done
mkuname "$pwsudo" Linux
mkid "$pwsudo" 1000    # non-root, whatever UID runs the suite
ln -sf "$perfbin/perf" "$pwsudo/perf"
pwtrip="$tmp/perf-pwsudo-tripwire.log"; : >"$pwtrip"
cat >"$pwsudo/sudo" <<EOF
#!/usr/bin/env bash
echo "sudo \$*" >>"$pwtrip"
exit 1
EOF
chmod +x "$pwsudo/sudo"
pwsudo_sysctl="$tmp/perf-pwsudo.d"; mkdir -p "$pwsudo_sysctl"
pwsudo_out=$(PATH="$pwsudo" HOME="$yes_home" CARGO_HOME="$yes_home/.cargo" \
  CQLITE_PERF_PROC_DIR="$perf_proc" CQLITE_PERF_SYSCTL_DIR="$pwsudo_sysctl" CQLITE_PERF_TEST_PRIV_DIR="$pwsudo" \
  bash "$tmp/perf-root-yes/scripts/bootstrap-agent-machine.sh" --skip-smoke --yes 2>&1)
pwsudo_rc=$?
if [ "$pwsudo_rc" -eq 0 ] && [ -z "$(ls -A "$pwsudo_sysctl")" ] \
   && printf '%s' "$pwsudo_out" | grep -q 'sudo needs a password' \
   && printf '%s' "$pwsudo_out" | grep -q 'sudo tee .*99-cqlite-perf.conf.*&& sudo sysctl -q --system' \
   && ! printf '%s' "$pwsudo_out" | grep -q "no 'sudo' binary"; then
  ok "perf section: a password-requiring sudo is diagnosed distinctly (not 'no sudo binary') with the interactive remedy, writes nothing, exits 0"
else
  bad "perf section: password-requiring sudo mishandled (rc=$pwsudo_rc, dir='$(ls -A "$pwsudo_sysctl")')"
  printf '%s\n' "$pwsudo_out" | sed -n '/Perf profiling/,/^$/p'
fi
# Whatever it invoked, it must have been non-interactive: EVERY recorded sudo line
# begins `sudo -n `, so no code path can sit on a password prompt on a worker.
pw_bad_sudo=$(sudo_perf_offenders "$pwtrip")
if grep -q '^sudo -n true$' "$pwtrip" && [ -z "$pw_bad_sudo" ]; then
  ok "perf section: every sudo invocation carries -n (never interactive)"
else
  bad "perf section: a sudo invocation without -n was recorded (or none at all): $(cat "$pwtrip")"
fi

# 4c-iii. ALREADY ROOT: the printed remedy must carry NO `sudo` prefix — many root
#         images have no sudo installed at all, so a hardcoded `sudo tee` line is
#         un-runnable exactly where it is printed. Check mode, so nothing is written.
rootbox="$tmp/perf-rootbox"; mkdir -p "$rootbox"
for t in bash cat sed awk grep printf tr cut sort head tail wc env date mktemp \
         diff timeout dirname basename ls rm mv cp mkdir touch git python3 hostname stat find tee; do
  s=$(command -v "$t" 2>/dev/null) && ln -sf "$s" "$rootbox/$t"
done
mkuname "$rootbox" Linux
mkid "$rootbox" 0      # the ONLY case that exercises the already-root branch
roottrip="$tmp/perf-rootbox-tripwire.log"; : >"$roottrip"
for t in sudo sysctl; do
  cat >"$rootbox/$t" <<EOF
#!/usr/bin/env bash
echo "$t \$*" >>"$roottrip"
exit 0
EOF
  chmod +x "$rootbox/$t"
done
ln -sf "$perfbin/perf" "$rootbox/perf"
mkperfshim 3333333
rootbox_d="$tmp/perf-rootbox.d"; mkdir -p "$rootbox_d"
rootbox_out=$(PATH="$rootbox" HOME="$yes_home" CARGO_HOME="$yes_home/.cargo" \
  CQLITE_PERF_PROC_DIR="$perf_proc" CQLITE_PERF_SYSCTL_DIR="$rootbox_d" CQLITE_PERF_TEST_PRIV_DIR="$rootbox" \
  bash "$tmp/perf-root-yes/scripts/bootstrap-agent-machine.sh" --skip-smoke 2>&1)
rootbox_rc=$?
rootbox_remedy=$(printf '%s\n' "$rootbox_out" | grep 'write + apply the drop-in' || true)
if [ "$rootbox_rc" -eq 0 ] && [ -z "$(ls -A "$rootbox_d")" ] \
   && printf '%s' "$rootbox_remedy" | grep -q '| tee .*99-cqlite-perf.conf >/dev/null && sysctl -q --system' \
   && ! printf '%s' "$rootbox_remedy" | grep -q 'sudo'; then
  ok "perf section: when ALREADY ROOT the printed write+apply remedy carries no 'sudo' prefix"
else
  bad "perf section: root-box remedy line wrong (rc=$rootbox_rc, line='$rootbox_remedy')"
fi

# 4d. THE SILENT REVERT — the real-world trap this whole issue exists to fix, and so
#      the path that must be best-tested. `sysctl` exits 0 while the value does NOT
#      take (container, read-only /proc, a later-sorting drop-in or /etc/sysctl.conf
#      winning), so the verdict may never come from the apply's return code. Here the
#      sysctl shim succeeds but does NOT touch the fake procfs, and perf is DENIED
#      (non-zero, as a real paranoid-4 box denies it): bootstrap must warn "did NOT
#      take", must NOT claim a READ BACK, and must NOT claim VERIFIED — while still
#      exiting 0. Trusting sysctl's rc keeps every other case green.
revert_proc="$tmp/perf-proc-revert"; mkdir -p "$revert_proc"
printf '4\n' >"$revert_proc/perf_event_paranoid"
printf '1\n' >"$revert_proc/kptr_restrict"
revert_shims="$tmp/perf-revert-shims"; mkdir -p "$revert_shims"
revert_trip="$tmp/perf-revert-tripwire.log"; : >"$revert_trip"
cat >"$revert_shims/sudo" <<EOF
#!/usr/bin/env bash
echo "sudo \$*" >>"$revert_trip"
while [ "\${1:-}" = "-n" ]; do shift; done
exec "\$@"
EOF
cat >"$revert_shims/sysctl" <<EOF
#!/usr/bin/env bash
echo "sysctl \$*" >>"$revert_trip"
exit 0
EOF
for t in apt-get apt dnf yum pacman; do
  printf '#!/usr/bin/env bash\nexit 0\n' >"$revert_shims/$t"
done
chmod +x "$revert_shims"/*
revert_sysctl_d="$tmp/perf-revert.d"; mkdir -p "$revert_sysctl_d"
mkperfshim_raw 1 'Error:' 'Access to performance monitoring and observability operations is limited.'
revert_out=$(PATH="$revert_shims:$perfbin:$tmp:$PATH" HOME="$yes_home" CARGO_HOME="$yes_home/.cargo" \
  CQLITE_PERF_PROC_DIR="$revert_proc" CQLITE_PERF_SYSCTL_DIR="$revert_sysctl_d" CQLITE_PERF_TEST_PRIV_DIR="$revert_shims" \
  bash "$tmp/perf-root-yes/scripts/bootstrap-agent-machine.sh" --skip-smoke --yes 2>&1)
revert_rc=$?
if [ "$revert_rc" -eq 0 ] \
   && printf '%s' "$revert_out" | grep -q 'the value did NOT take' \
   && printf '%s' "$revert_out" | grep -q 'perf=paranoid-4' \
   && ! printf '%s' "$revert_out" | grep -q 'READ BACK from /proc' \
   && ! printf '%s' "$revert_out" | grep -q 'perf capability VERIFIED' \
   && grep -q 'sysctl -q --system' "$revert_trip"; then
  ok "perf section: a sysctl that exits 0 without the value taking is reported as 'did NOT take' from /proc (no READ BACK, no VERIFIED), rc 0"
else
  bad "perf section: the silent revert was not caught (rc=$revert_rc)"
  printf '%s\n' "$revert_out" | sed -n '/Perf profiling/,/^$/p'
fi
# ...and the diagnostics must name /etc/sysctl.conf, which BOTH `sysctl --system` and
# systemd-sysctl apply AFTER the sysctl.d drop-ins — so a stale entry there beats our
# 99- file and is the likeliest cause of exactly this state.
if printf '%s' "$revert_out" | grep -q '/etc/sysctl.conf' \
   && printf '%s' "$revert_out" | grep -qi 'applied AFTER the sysctl.d drop-ins'; then
  ok "perf section: the did-NOT-take diagnostics name /etc/sysctl.conf and its precedence over the drop-ins"
else
  bad "perf section: diagnostics omit the /etc/sysctl.conf precedence trap"
fi

# 4d-ii. CHECK mode with the drop-in already on disk but the runtime NOT profileable:
#         the apply was only PRINTED, so bootstrap must NOT claim it was applied, and
#         must still WARN (this branch was previously unreachable — the mismatch
#         produced no [warn] at all and counted no WARNING).
checkapply_d="$tmp/perf-checkapply.d"; mkdir -p "$checkapply_d"
bash "$PERFLIB" --drop-in >"$checkapply_d/99-cqlite-perf.conf"
checkapply_trip="$tmp/perf-checkapply-tripwire.log"; : >"$checkapply_trip"
checkapply_shims="$tmp/perf-checkapply-shims"; mkdir -p "$checkapply_shims"
for t in sudo sysctl tee; do
  cat >"$checkapply_shims/$t" <<EOF
#!/usr/bin/env bash
echo "$t \$*" >>"$checkapply_trip"
exit 0
EOF
done
chmod +x "$checkapply_shims"/*
mkperfshim 4242
checkapply_out=$(PATH="$checkapply_shims:$perfbin:$tmp:$PATH" HOME="$yes_home" CARGO_HOME="$yes_home/.cargo" \
  CQLITE_PERF_PROC_DIR="$revert_proc" CQLITE_PERF_SYSCTL_DIR="$checkapply_d" CQLITE_PERF_TEST_PRIV_DIR="$checkapply_shims" \
  bash "$tmp/perf-root-yes/scripts/bootstrap-agent-machine.sh" --skip-smoke 2>&1)
checkapply_rc=$?
checkapply_mutating=$(grep -vE '^sudo -n true$' "$checkapply_trip" | grep -E '^(sudo|sysctl|tee) ' | head -5)
if [ "$checkapply_rc" -eq 0 ] \
   && printf '%s' "$checkapply_out" | grep -q 'drop-in already current' \
   && printf '%s' "$checkapply_out" | grep -q 'has NOT been applied to the running kernel' \
   && ! printf '%s' "$checkapply_out" | grep -q 'READ BACK from /proc' \
   && ! printf '%s' "$checkapply_out" | grep -q 'sysctl --system reported success' \
   && [ -z "$checkapply_mutating" ]; then
  ok "perf section: check mode with a current drop-in but a non-ok runtime WARNs that it is not applied (and applies nothing)"
else
  bad "perf section: check-mode drop-in/runtime mismatch mishandled (rc=$checkapply_rc, tripwire='$checkapply_mutating')"
  printf '%s\n' "$checkapply_out" | sed -n '/Perf profiling/,/^$/p'
fi

# 4d-iii. THE APPLY COMMAND FAILED, THE CONTROLS TOOK ANYWAY. `sysctl --system` applies
#         EVERY drop-in on the box, so it can apply OURS correctly and still exit
#         non-zero because an unrelated pre-existing entry failed (a stale
#         /etc/sysctl.conf line, a foreign drop-in naming a knob this kernel lacks) —
#         and one such entry anywhere on a fleet box is enough. Gating the /proc
#         read-back on that rc left the token STALE and printed "nothing was applied"
#         about a box that had JUST become profileable: a false verdict, which is the
#         one thing AC2 exists to prevent. The two facts are independent and must be
#         reported independently — the command's failure named as the COMMAND's, and the
#         capability verdict taken from /proc regardless. Here the sysctl shim exits 1
#         AND updates the fake procfs.
rcfail_proc="$tmp/perf-proc-rcfail"; mkdir -p "$rcfail_proc"
printf '4\n' >"$rcfail_proc/perf_event_paranoid"
printf '1\n' >"$rcfail_proc/kptr_restrict"
rcfail_shims="$tmp/perf-rcfail-shims"; mkdir -p "$rcfail_shims"
rcfail_trip="$tmp/perf-rcfail-tripwire.log"; : >"$rcfail_trip"
cat >"$rcfail_shims/sudo" <<EOF
#!/usr/bin/env bash
echo "sudo \$*" >>"$rcfail_trip"
while [ "\${1:-}" = "-n" ]; do shift; done
exec "\$@"
EOF
cat >"$rcfail_shims/sysctl" <<EOF
#!/usr/bin/env bash
echo "sysctl \$*" >>"$rcfail_trip"
case " \$* " in *" --system "*)
  printf -- '-1\n' >"$rcfail_proc/perf_event_paranoid"
  printf '0\n'     >"$rcfail_proc/kptr_restrict"
  echo "sysctl: setting key \"vm.unrelated_stale_entry\": Invalid argument" >&2 ;;
esac
exit 1
EOF
for t in apt-get apt dnf yum pacman; do
  printf '#!/usr/bin/env bash\nexit 0\n' >"$rcfail_shims/$t"
done
chmod +x "$rcfail_shims"/*
rcfail_d="$tmp/perf-rcfail.d"; mkdir -p "$rcfail_d"
bash "$PERFLIB" --drop-in >"$rcfail_d/99-cqlite-perf.conf"   # already current: only the apply is left
mkperfshim 6060606
rcfail_out=$(PATH="$rcfail_shims:$perfbin:$tmp:$PATH" HOME="$yes_home" CARGO_HOME="$yes_home/.cargo" \
  CQLITE_PERF_PROC_DIR="$rcfail_proc" CQLITE_PERF_SYSCTL_DIR="$rcfail_d" CQLITE_PERF_TEST_PRIV_DIR="$rcfail_shims" \
  bash "$tmp/perf-root-yes/scripts/bootstrap-agent-machine.sh" --skip-smoke --yes 2>&1)
rcfail_rc=$?
# The read-back verdict must be GOOD (it is what /proc now says), the command failure
# must be reported DISTINCTLY (naming the exit status and that it applies every
# drop-in), and the two must not contradict: no "nothing was applied", no "did NOT
# take", no "NOT in the profileable state" alongside a good read-back. rc still 0.
if [ "$rcfail_rc" -eq 0 ] \
   && printf '%s' "$rcfail_out" | grep -q "READ BACK from /proc as profileable: perf_event_paranoid=-1 kptr_restrict=0" \
   && printf '%s' "$rcfail_out" | grep -q "'sysctl -q --system' exited 1" \
   && printf '%s' "$rcfail_out" | grep -q 'UNRELATED pre-existing entry' \
   && ! printf '%s' "$rcfail_out" | grep -q 'nothing was applied' \
   && ! printf '%s' "$rcfail_out" | grep -q 'the value did NOT take' \
   && ! printf '%s' "$rcfail_out" | grep -q 'NOT in the profileable state' \
   && ! printf '%s' "$rcfail_out" | grep -q 'are NOT in effect' \
   && grep -q 'sysctl -q --system' "$rcfail_trip"; then
  ok "perf section: a sysctl that FAILS while the controls DO take reports the command failure distinctly and still reads the good verdict back from /proc, rc 0"
else
  bad "perf section: a failing sysctl suppressed the /proc read-back or contradicted it (rc=$rcfail_rc)"
  printf '%s\n' "$rcfail_out" | sed -n '/Perf profiling/,/^$/p'
fi
# ...and the run must still be honest when BOTH facts are bad: the same failing sysctl
# with a procfs that does NOT change must name the command failure AND report the
# controls as not in effect — never a good read-back.
rcfail2_proc="$tmp/perf-proc-rcfail2"; mkdir -p "$rcfail2_proc"
printf '4\n' >"$rcfail2_proc/perf_event_paranoid"
printf '1\n' >"$rcfail2_proc/kptr_restrict"
rcfail2_shims="$tmp/perf-rcfail2-shims"; mkdir -p "$rcfail2_shims"
cp "$rcfail_shims/sudo" "$rcfail2_shims/sudo"
printf '#!/usr/bin/env bash\nexit 1\n' >"$rcfail2_shims/sysctl"
for t in apt-get apt dnf yum pacman; do
  printf '#!/usr/bin/env bash\nexit 0\n' >"$rcfail2_shims/$t"
done
chmod +x "$rcfail2_shims"/*
rcfail2_out=$(PATH="$rcfail2_shims:$perfbin:$tmp:$PATH" HOME="$yes_home" CARGO_HOME="$yes_home/.cargo" \
  CQLITE_PERF_PROC_DIR="$rcfail2_proc" CQLITE_PERF_SYSCTL_DIR="$rcfail_d" CQLITE_PERF_TEST_PRIV_DIR="$rcfail2_shims" \
  bash "$tmp/perf-root-yes/scripts/bootstrap-agent-machine.sh" --skip-smoke --yes 2>&1)
rcfail2_rc=$?
if [ "$rcfail2_rc" -eq 0 ] \
   && printf '%s' "$rcfail2_out" | grep -q "'sysctl -q --system' exited 1" \
   && printf '%s' "$rcfail2_out" | grep -q 'the controls are NOT in effect' \
   && ! printf '%s' "$rcfail2_out" | grep -q 'READ BACK from /proc as profileable' \
   && ! printf '%s' "$rcfail2_out" | grep -q 'reported success'; then
  ok "perf section: a failing sysctl whose controls did NOT take reports BOTH facts (command exited 1 + controls not in effect), never a good read-back"
else
  bad "perf section: the both-bad case mishandled (rc=$rcfail2_rc)"
  printf '%s\n' "$rcfail2_out" | sed -n '/Perf profiling/,/^$/p'
fi

# 4d-iv. CURRENT DROP-IN + NO PRIVILEGE + non-ok runtime: nothing to WRITE (so the
#        write remedy is not the fix) and no non-interactive privilege to APPLY, which
#        used to print a diagnosis with NO remedy at all — contradicting every other
#        unprivileged path in this section. Two boxes, two remedies: a box with no sudo
#        BINARY needs the root-shell line (a `sudo` line is un-runnable there), a box
#        whose sudo needs a password needs the same line WITH `sudo` (it works
#        interactively).
noapply_d="$tmp/perf-noapply.d"; mkdir -p "$noapply_d"
bash "$PERFLIB" --drop-in >"$noapply_d/99-cqlite-perf.conf"
noapply_ref="$tmp/perf-noapply-ref.conf"; cp "$noapply_d/99-cqlite-perf.conf" "$noapply_ref"
noapply_out=$(PATH="$nosudo" HOME="$yes_home" CARGO_HOME="$yes_home/.cargo" \
  CQLITE_PERF_PROC_DIR="$perf_proc" CQLITE_PERF_SYSCTL_DIR="$noapply_d" CQLITE_PERF_TEST_PRIV_DIR="$nosudo" \
  bash "$tmp/perf-root-yes/scripts/bootstrap-agent-machine.sh" --skip-smoke --yes 2>&1)
noapply_rc=$?
if [ "$noapply_rc" -eq 0 ] \
   && cmp -s "$noapply_ref" "$noapply_d/99-cqlite-perf.conf" \
   && printf '%s' "$noapply_out" | grep -q 'drop-in already current' \
   && printf '%s' "$noapply_out" | grep -q 'NOT in the profileable state' \
   && printf '%s' "$noapply_out" | grep -q "no 'sudo' on this box — apply it from a ROOT shell:  sysctl -q --system" \
   && ! printf '%s' "$noapply_out" | grep -q 'sudo sysctl'; then
  ok "perf section: a current drop-in that cannot be applied (no sudo binary) still prints a RUNNABLE root-shell apply remedy, writes nothing, exits 0"
else
  bad "perf section: the current-drop-in/no-privilege branch printed no usable apply remedy (rc=$noapply_rc)"
  printf '%s\n' "$noapply_out" | sed -n '/Perf profiling/,/^$/p'
fi
noapply2_out=$(PATH="$pwsudo" HOME="$yes_home" CARGO_HOME="$yes_home/.cargo" \
  CQLITE_PERF_PROC_DIR="$perf_proc" CQLITE_PERF_SYSCTL_DIR="$noapply_d" CQLITE_PERF_TEST_PRIV_DIR="$pwsudo" \
  bash "$tmp/perf-root-yes/scripts/bootstrap-agent-machine.sh" --skip-smoke --yes 2>&1)
noapply2_rc=$?
if [ "$noapply2_rc" -eq 0 ] \
   && cmp -s "$noapply_ref" "$noapply_d/99-cqlite-perf.conf" \
   && printf '%s' "$noapply2_out" | grep -q 'NOT in the profileable state' \
   && printf '%s' "$noapply2_out" | grep -q 'apply the drop-in now:  sudo sysctl -q --system' \
   && ! printf '%s' "$noapply2_out" | grep -q "no 'sudo' on this box"; then
  ok "perf section: the same branch on a password-requiring sudo prints the INTERACTIVE 'sudo sysctl -q --system' apply remedy"
else
  bad "perf section: the password-sudo current-drop-in branch remedy is wrong (rc=$noapply2_rc)"
  printf '%s\n' "$noapply2_out" | sed -n '/Perf profiling/,/^$/p'
fi

# 4e. A box with NO `perf` at all must WARN + print the install remedy and STILL exit
#      0 — bootstrap is the fleet provisioning entry point, so a hard exit here would
#      break every box without linux-tools. Every other case has perf on PATH, so an
#      `exit 1` inserted in this branch previously survived the whole suite.
noperfbox="$tmp/perf-noperfbox"; mkdir -p "$noperfbox"
for t in bash cat sed awk grep printf tr cut sort head tail wc env date mktemp \
         diff timeout dirname basename ls rm mv cp mkdir touch git python3 hostname stat find tee; do
  s=$(command -v "$t" 2>/dev/null) && ln -sf "$s" "$noperfbox/$t"
done
mkuname "$noperfbox" Linux
mkid "$noperfbox" 1000    # non-root, whatever UID runs the suite
cat >"$noperfbox/sudo" <<EOF
#!/usr/bin/env bash
while [ "\${1:-}" = "-n" ]; do shift; done
exec "\$@"
EOF
printf '#!/usr/bin/env bash\nexit 0\n' >"$noperfbox/sysctl"
printf '#!/usr/bin/env bash\nexit 0\n' >"$noperfbox/apt-get"
chmod +x "$noperfbox/sudo" "$noperfbox/sysctl" "$noperfbox/apt-get"
noperfbox_d="$tmp/perf-noperfbox.d"; mkdir -p "$noperfbox_d"
noperfbox_out=$(PATH="$noperfbox" HOME="$yes_home" CARGO_HOME="$yes_home/.cargo" \
  CQLITE_PERF_PROC_DIR="$revert_proc" CQLITE_PERF_SYSCTL_DIR="$noperfbox_d" CQLITE_PERF_TEST_PRIV_DIR="$noperfbox" \
  bash "$tmp/perf-root-yes/scripts/bootstrap-agent-machine.sh" --skip-smoke --yes 2>&1)
noperfbox_rc=$?
if [ "$noperfbox_rc" -eq 0 ] \
   && printf '%s' "$noperfbox_out" | grep -q 'perf MISSING' \
   && printf '%s' "$noperfbox_out" | grep -q 'install perf:.*linux-tools' \
   && ! printf '%s' "$noperfbox_out" | grep -q 'perf capability VERIFIED'; then
  ok "perf section: a box with no 'perf' binary warns UNVERIFIED + prints the linux-tools remedy and still exits 0"
else
  bad "perf section: missing-perf box mishandled (rc=$noperfbox_rc)"
  printf '%s\n' "$noperfbox_out" | sed -n '/Perf profiling/,/^$/p'
fi

# 4f. DARWIN no-op: perf_event_paranoid/kptr_restrict are Linux kernel controls, so on
#      macOS the section must say so, write nothing, and exit 0. Without a `uname`
#      stub this whole suite was silently Linux-host-only (a Darwin gate host would
#      have reddened `tooling-tests` on 10 cases while this path had no assertion).
darwin_dir="$tmp/perf-darwin-bin"; mkdir -p "$darwin_dir"
mkuname "$darwin_dir" Darwin
darwin_d="$tmp/perf-darwin.d"; mkdir -p "$darwin_d"
darwin_trip="$tmp/perf-darwin-tripwire.log"; : >"$darwin_trip"
for t in sudo sysctl tee; do
  cat >"$darwin_dir/$t" <<EOF
#!/usr/bin/env bash
echo "$t \$*" >>"$darwin_trip"
exit 0
EOF
done
chmod +x "$darwin_dir"/*
darwin_out=$(PATH="$darwin_dir:$perfbin:$tmp:$PATH" HOME="$yes_home" CARGO_HOME="$yes_home/.cargo" \
  CQLITE_PERF_PROC_DIR="$revert_proc" CQLITE_PERF_SYSCTL_DIR="$darwin_d" CQLITE_PERF_TEST_PRIV_DIR="$darwin_dir" \
  bash "$tmp/perf-root-yes/scripts/bootstrap-agent-machine.sh" --skip-smoke --yes 2>&1)
darwin_rc=$?
darwin_mutating=$(grep -vE '^sudo -n true$' "$darwin_trip" | grep -E '^(sudo|sysctl|tee) ' | head -5)
if [ "$darwin_rc" -eq 0 ] \
   && printf '%s' "$darwin_out" | grep -q 'nothing to configure on macos' \
   && [ -z "$(ls -A "$darwin_d")" ] && [ -z "$darwin_mutating" ] \
   && ! printf '%s' "$darwin_out" | grep -q 'perf capability VERIFIED'; then
  ok "perf section: on Darwin the section is an explicit no-op — no write, no privileged call, rc 0"
else
  bad "perf section: Darwin no-op mishandled (rc=$darwin_rc, dir='$(ls -A "$darwin_d")', tripwire='$darwin_mutating')"
  printf '%s\n' "$darwin_out" | sed -n '/Perf profiling/,/^$/p'
fi

# 4f-ii. AN INHERITED `PERF_SECTION_OK=1` MAY NOT STEER THE SECTION (issue #3249
#        review). The gate was read as `${PERF_SECTION_OK:-0}` with no initialisation
#        before the platform/library checks, so an ambient export carried a macOS host
#        straight into the LINUX-ONLY implementation and called helper functions that
#        were never sourced. Same env-inheritance class as the CQLITE_PERF_SYSCTL_DIR
#        seam steering a privileged write. Asserted on BOTH pre-conditions the guard
#        chain has: the wrong platform, and a checkout with no perf-capability.sh.
darwin_inherit_out=$(PERF_SECTION_OK=1 PATH="$darwin_dir:$perfbin:$tmp:$PATH" \
  HOME="$yes_home" CARGO_HOME="$yes_home/.cargo" \
  CQLITE_PERF_PROC_DIR="$revert_proc" CQLITE_PERF_SYSCTL_DIR="$darwin_d" CQLITE_PERF_TEST_PRIV_DIR="$darwin_dir" \
  bash "$tmp/perf-root-yes/scripts/bootstrap-agent-machine.sh" --skip-smoke --yes 2>&1)
darwin_inherit_rc=$?
if [ "$darwin_inherit_rc" -eq 0 ] \
   && printf '%s' "$darwin_inherit_out" | grep -q 'nothing to configure on macos' \
   && [ -z "$(ls -A "$darwin_d")" ] \
   && ! printf '%s' "$darwin_inherit_out" | grep -q 'runtime now: perf_event_paranoid' \
   && ! printf '%s' "$darwin_inherit_out" | grep -q 'perf capability VERIFIED' \
   && ! printf '%s' "$darwin_inherit_out" | grep -qi 'command not found'; then
  ok "perf section: an INHERITED PERF_SECTION_OK=1 cannot drag a Darwin run into the Linux-only implementation"
else
  bad "perf section: an inherited PERF_SECTION_OK=1 steered the section (rc=$darwin_inherit_rc)"
  printf '%s\n' "$darwin_inherit_out" | sed -n '/Perf profiling/,/^$/p'
fi
nolib_root="$tmp/perf-root-nolib"; mkdir -p "$nolib_root/scripts/lib"
cp "$BOOTSTRAP" "$nolib_root/scripts/bootstrap-agent-machine.sh"
cp "$SCRIPT_DIR/../lib/gate-notify.sh" "$nolib_root/scripts/lib/gate-notify.sh" 2>/dev/null || true
nolib_d="$tmp/perf-nolib.d"; mkdir -p "$nolib_d"
nolib_out=$(PERF_SECTION_OK=1 PATH="$checkapply_shims:$perfbin:$tmp:$PATH" \
  HOME="$yes_home" CARGO_HOME="$yes_home/.cargo" \
  CQLITE_PERF_PROC_DIR="$revert_proc" CQLITE_PERF_SYSCTL_DIR="$nolib_d" CQLITE_PERF_TEST_PRIV_DIR="$checkapply_shims" \
  bash "$nolib_root/scripts/bootstrap-agent-machine.sh" --skip-smoke --yes 2>&1)
nolib_rc=$?
if [ "$nolib_rc" -eq 0 ] \
   && printf '%s' "$nolib_out" | grep -q 'perf-capability.sh missing from this checkout' \
   && [ -z "$(ls -A "$nolib_d")" ] \
   && ! printf '%s' "$nolib_out" | grep -q 'runtime now: perf_event_paranoid' \
   && ! printf '%s' "$nolib_out" | grep -q 'perf capability VERIFIED' \
   && ! printf '%s' "$nolib_out" | grep -qi 'command not found'; then
  ok "perf section: an INHERITED PERF_SECTION_OK=1 cannot enter the implementation with no perf-capability.sh in the checkout"
else
  bad "perf section: an inherited PERF_SECTION_OK=1 entered the section without its library (rc=$nolib_rc)"
  printf '%s\n' "$nolib_out" | sed -n '/Perf profiling/,/^$/p'
fi

# --- 5. THE FUNCTIONAL RESULT IS SUBORDINATE TO /proc, AND TO IDENTITY --------
# 5a. A SUCCESSFUL perf stat while /proc says paranoid-4. Reporting the functional
#     result as the overall verdict let a run print a `paranoid-*` diagnosis AND
#     "VERIFIED" in the same output — contradictory, with the reassuring line winning
#     the reader's attention. Overall verification now requires BOTH facts; a lone
#     functional pass is PARTIAL DIAGNOSTIC INFORMATION and /proc governs.
mkperfshim 8888888
subord_d="$tmp/perf-subord.d"; mkdir -p "$subord_d"
bash "$PERFLIB" --drop-in >"$subord_d/99-cqlite-perf.conf"   # current: only /proc disagrees
subord_out=$(PATH="$checkapply_shims:$perfbin:$tmp:$PATH" HOME="$yes_home" CARGO_HOME="$yes_home/.cargo" \
  CQLITE_PERF_PROC_DIR="$revert_proc" CQLITE_PERF_SYSCTL_DIR="$subord_d" CQLITE_PERF_TEST_PRIV_DIR="$checkapply_shims" \
  bash "$tmp/perf-root-yes/scripts/bootstrap-agent-machine.sh" --skip-smoke 2>&1)
subord_rc=$?
if [ "$subord_rc" -eq 0 ] \
   && ! printf '%s' "$subord_out" | grep -q 'perf capability VERIFIED' \
   && printf '%s' "$subord_out" | grep -q 'perf capability NOT verified' \
   && printf '%s' "$subord_out" | grep -q 'PARTIAL DIAGNOSTIC INFORMATION' \
   && printf '%s' "$subord_out" | grep -q '/proc is the AUTHORITY here: perf=paranoid-4' \
   && printf '%s' "$subord_out" | grep -qi 'PERMISSION verdict' \
   && printf '%s' "$subord_out" | grep -q 'cycles=8888888'; then
  ok "perf section: a SUCCESSFUL perf stat while /proc says paranoid-4 is never 'VERIFIED' — reported as partial info, /proc governs"
else
  bad "perf section: a functional pass overrode a non-ok /proc verdict (rc=$subord_rc)"
  printf '%s\n' "$subord_out" | sed -n '/Perf profiling/,/^$/p'
fi

# 5a-ii. The same rule on the OTHER non-ok token: kptr_restrict != 0 costs kernel
#        SYMBOLS silently, and `perf stat -C 0 -e cycles` counts perfectly well without
#        them — so this is the token most likely to be masked by a functional pass.
kptr_proc="$tmp/perf-proc-kptr"; mkdir -p "$kptr_proc"
printf -- '-1\n' >"$kptr_proc/perf_event_paranoid"
printf '1\n'     >"$kptr_proc/kptr_restrict"
kptr_out=$(PATH="$checkapply_shims:$perfbin:$tmp:$PATH" HOME="$yes_home" CARGO_HOME="$yes_home/.cargo" \
  CQLITE_PERF_PROC_DIR="$kptr_proc" CQLITE_PERF_SYSCTL_DIR="$subord_d" CQLITE_PERF_TEST_PRIV_DIR="$checkapply_shims" \
  bash "$tmp/perf-root-yes/scripts/bootstrap-agent-machine.sh" --skip-smoke 2>&1)
kptr_rc=$?
if [ "$kptr_rc" -eq 0 ] \
   && ! printf '%s' "$kptr_out" | grep -q 'perf capability VERIFIED' \
   && printf '%s' "$kptr_out" | grep -q 'perf capability NOT verified' \
   && printf '%s' "$kptr_out" | grep -q '/proc is the AUTHORITY here: perf=kptr-restricted' \
   && printf '%s' "$kptr_out" | grep -q 'kptr_restrict != 0'; then
  ok "perf section: a SUCCESSFUL perf stat while /proc says kptr-restricted is never 'VERIFIED' (the silent symbol loss still governs)"
else
  bad "perf section: a functional pass masked a kptr-restricted verdict (rc=$kptr_rc)"
  printf '%s\n' "$kptr_out" | sed -n '/Perf profiling/,/^$/p'
fi

# 5b. THE PRIVILEGE DIMENSION — the most consequential case in this suite.
#     perf_event_paranoid restricts UNPRIVILEGED users; ROOT BYPASSES IT. So under
#     `sudo bash scripts/bootstrap-agent-machine.sh` — a normal provisioning
#     invocation, and the likeliest one since writing /etc/sysctl.d needs root — a
#     root `perf stat -C 0 -e cycles` SUCCEEDS on a paranoid=4 box where every
#     unprivileged agent still gets EACCES. Bootstrap must therefore probe as an
#     UNPRIVILEGED identity. Here the box offers `setpriv`, /proc is ALREADY ok, and
#     the drop-in is current: the run must (1) say it dropped privilege, (2) actually
#     route the collection through setpriv with the resolved uid/gid, and (3) only
#     THEN report VERIFIED.
droproot="$tmp/perf-droproot"; mkdir -p "$droproot"
for t in bash cat sed awk grep printf tr cut sort head tail wc env date mktemp \
         diff timeout dirname basename ls rm mv cp mkdir touch git python3 hostname stat find tee; do
  s=$(command -v "$t" 2>/dev/null) && ln -sf "$s" "$droproot/$t"
done
mkuname "$droproot" Linux
mkid "$droproot" 0 1000       # we are root; `nobody` resolves to the unprivileged 1000
droptrip="$tmp/perf-droproot-tripwire.log"; : >"$droptrip"
# setpriv shim: records the exact drop it was asked for, then execs the collection —
# so the assertion below proves the PROBE went through it, not merely that a line
# claiming so was printed.
cat >"$droproot/setpriv" <<EOF
#!/usr/bin/env bash
echo "setpriv \$*" >>"$droptrip"
while [ \$# -gt 0 ]; do case "\$1" in --*) shift ;; *) break ;; esac; done
exec "\$@"
EOF
printf '#!/usr/bin/env bash\necho "sysctl $*" >>"%s"\nexit 0\n' "$droptrip" >"$droproot/sysctl"
chmod +x "$droproot/setpriv" "$droproot/sysctl"
ln -sf "$perfbin/perf" "$droproot/perf"
mkperfshim 1212121
drop_proc="$tmp/perf-proc-drop"; mkdir -p "$drop_proc"
printf -- '-1\n' >"$drop_proc/perf_event_paranoid"
printf '0\n'     >"$drop_proc/kptr_restrict"
drop_d="$tmp/perf-drop.d"; mkdir -p "$drop_d"
bash "$PERFLIB" --drop-in >"$drop_d/99-cqlite-perf.conf"
drop_out=$(PATH="$droproot" HOME="$yes_home" CARGO_HOME="$yes_home/.cargo" \
  CQLITE_PERF_PROC_DIR="$drop_proc" CQLITE_PERF_SYSCTL_DIR="$drop_d" CQLITE_PERF_TEST_PRIV_DIR="$droproot" \
  bash "$tmp/perf-root-yes/scripts/bootstrap-agent-machine.sh" --skip-smoke 2>&1)
drop_rc=$?
if [ "$drop_rc" -eq 0 ] \
   && printf '%s' "$drop_out" | grep -q 'this run is ROOT and root BYPASSES perf_event_paranoid' \
   && printf '%s' "$drop_out" | grep -q 'DROPS PRIVILEGE (dropped:setpriv:uid=1000)' \
   && grep -q 'setpriv --reuid=1000 --regid=1000 --clear-groups' "$droptrip" \
   && printf '%s' "$drop_out" | grep -q 'perf capability VERIFIED .*UNPRIVILEGED perf stat -C 0 -e cycles reports cycles=1212121'; then
  ok "perf section: a ROOT run DROPS PRIVILEGE for the probe (setpriv, resolved uid/gid) and only then reports VERIFIED"
else
  bad "perf section: the root run did not probe as an unprivileged identity (rc=$drop_rc, tripwire='$(cat "$droptrip")')"
  printf '%s\n' "$drop_out" | sed -n '/Perf profiling/,/^$/p'
fi

# 5b-ii. THE FALSE VERIFICATION ITSELF: the same ALREADY-ok box, the same succeeding
#        perf — but no setpriv/runuser/sudo to drop privilege with. The functional
#        result then says nothing about an unprivileged process, and reporting it as
#        "VERIFIED" is exactly the false verification of an unprofileable box. It must
#        be labelled as NOT evidence, with /proc left as the authority.
nodroproot="$tmp/perf-nodroproot"; mkdir -p "$nodroproot"
for t in bash cat sed awk grep printf tr cut sort head tail wc env date mktemp \
         diff timeout dirname basename ls rm mv cp mkdir touch git python3 hostname stat find tee; do
  s=$(command -v "$t" 2>/dev/null) && ln -sf "$s" "$nodroproot/$t"
done
mkuname "$nodroproot" Linux
mkid "$nodroproot" 0 1000     # root, an unprivileged target exists, but no mechanism
nodroptrip="$tmp/perf-nodroproot-tripwire.log"; : >"$nodroptrip"
printf '#!/usr/bin/env bash\necho "sysctl $*" >>"%s"\nexit 0\n' "$nodroptrip" >"$nodroproot/sysctl"
chmod +x "$nodroproot/sysctl"
ln -sf "$perfbin/perf" "$nodroproot/perf"
mkperfshim 2323232
nodrop_out=$(PATH="$nodroproot" HOME="$yes_home" CARGO_HOME="$yes_home/.cargo" \
  CQLITE_PERF_PROC_DIR="$drop_proc" CQLITE_PERF_SYSCTL_DIR="$drop_d" CQLITE_PERF_TEST_PRIV_DIR="$nodroproot" \
  bash "$tmp/perf-root-yes/scripts/bootstrap-agent-machine.sh" --skip-smoke 2>&1)
nodrop_rc=$?
if [ "$nodrop_rc" -eq 0 ] \
   && ! printf '%s' "$nodrop_out" | grep -q 'perf capability VERIFIED' \
   && printf '%s' "$nodrop_out" | grep -q 'perf capability NOT verified' \
   && printf '%s' "$nodrop_out" | grep -q 'ran AS ROOT (root-no-drop-mechanism) and root BYPASSES perf_event_paranoid' \
   && printf '%s' "$nodrop_out" | grep -q 'NOT evidence that an UNPRIVILEGED process can profile this box' \
   && printf '%s' "$nodrop_out" | grep -q 'sudo -u <agent-user> perf stat'; then
  ok "perf section: a ROOT probe with NO way to drop privilege is labelled NOT evidence of unprivileged capability (never 'VERIFIED'), even with /proc ok"
else
  bad "perf section: a root-only functional pass was reported as verification (rc=$nodrop_rc)"
  printf '%s\n' "$nodrop_out" | sed -n '/Perf profiling/,/^$/p'
fi

# 5b-iii. ...and with NO unprivileged identity resolvable at all (no `nobody` on the
#         box, no SUDO_UID), the state is distinct — nothing to probe AS, so nothing
#         to claim. Root must not fall back to probing as itself and calling it good.
notarget="$tmp/perf-notarget"; mkdir -p "$notarget"
for t in bash cat sed awk grep printf tr cut sort head tail wc env date mktemp \
         diff timeout dirname basename ls rm mv cp mkdir touch git python3 hostname stat find tee; do
  s=$(command -v "$t" 2>/dev/null) && ln -sf "$s" "$notarget/$t"
done
mkuname "$notarget" Linux
mkid "$notarget" 0            # root, and `id -u nobody` FAILS: no unprivileged account
printf '#!/usr/bin/env bash\nexit 0\n' >"$notarget/sysctl"
chmod +x "$notarget/sysctl"
ln -sf "$perfbin/perf" "$notarget/perf"
ln -sf "$droproot/setpriv" "$notarget/setpriv"   # a mechanism exists; a TARGET does not
mkperfshim 3434343
notarget_out=$(PATH="$notarget" HOME="$yes_home" CARGO_HOME="$yes_home/.cargo" \
  CQLITE_PERF_PROC_DIR="$drop_proc" CQLITE_PERF_SYSCTL_DIR="$drop_d" CQLITE_PERF_TEST_PRIV_DIR="$notarget" \
  bash "$tmp/perf-root-yes/scripts/bootstrap-agent-machine.sh" --skip-smoke 2>&1)
notarget_rc=$?
if [ "$notarget_rc" -eq 0 ] \
   && ! printf '%s' "$notarget_out" | grep -q 'perf capability VERIFIED' \
   && printf '%s' "$notarget_out" | grep -q 'ran AS ROOT (root-no-unprivileged-target)'; then
  ok "perf section: root with no resolvable unprivileged identity reports that distinctly and claims no verification"
else
  bad "perf section: root with no unprivileged target mishandled (rc=$notarget_rc)"
  printf '%s\n' "$notarget_out" | sed -n '/Perf profiling/,/^$/p'
fi

# 5b-iv. SUDO_UID is preferred over `nobody`: under `sudo bootstrap` it names the
#        account whose profiling capability is actually in question, which is stronger
#        evidence than an unrelated system account.
sudouid_out=$(PATH="$droproot" HOME="$yes_home" CARGO_HOME="$yes_home/.cargo" \
  SUDO_UID=1234 SUDO_GID=1235 SUDO_USER=agentuser \
  CQLITE_PERF_PROC_DIR="$drop_proc" CQLITE_PERF_SYSCTL_DIR="$drop_d" CQLITE_PERF_TEST_PRIV_DIR="$droproot" \
  bash "$tmp/perf-root-yes/scripts/bootstrap-agent-machine.sh" --skip-smoke 2>&1)
sudouid_rc=$?
if [ "$sudouid_rc" -eq 0 ] \
   && printf '%s' "$sudouid_out" | grep -q 'DROPS PRIVILEGE (dropped:setpriv:uid=1234)' \
   && grep -q 'setpriv --reuid=1234 --regid=1235 --clear-groups' "$droptrip"; then
  ok "perf section: under sudo the probe drops to SUDO_UID/SUDO_GID (the account actually in question), not to nobody"
else
  bad "perf section: SUDO_UID was not preferred as the probe identity (rc=$sudouid_rc)"
  printf '%s\n' "$sudouid_out" | sed -n '/Perf profiling/,/^$/p'
fi

# 5c. The helper's identity-aware CLI, directly: `--verify-unpriv` must FAIL when the
#     result cannot be attributed to an unprivileged process, even though the very same
#     collection succeeded — and must report which state it was in. `--verify` keeps its
#     narrower contract (this identity, whoever that is), so the two are not confusable.
mkperfshim 5151515
vu_self=$(PATH="$perfbin:$tmp:$PATH" bash "$PERFLIB" --verify-unpriv 2>&1); vu_self_rc=$?
vu_root=$(PATH="$nodroproot" bash "$PERFLIB" --verify-unpriv 2>&1); vu_root_rc=$?
if [ "$vu_self_rc" -eq 0 ] && printf '%s' "$vu_self" | grep -q 'cycles=5151515 identity=self-unprivileged' \
   && [ "$vu_root_rc" -ne 0 ] && printf '%s' "$vu_root" | grep -q 'cycles=5151515 identity=root-no-drop-mechanism'; then
  ok "perf-capability: --verify-unpriv passes only when the result is attributable to an unprivileged identity (and names the state)"
else
  bad "perf-capability: --verify-unpriv identity attribution wrong (self rc=$vu_self_rc '$vu_self'; root rc=$vu_root_rc '$vu_root')"
fi

# 4g. Nothing in this whole suite may have touched the REAL /etc/sysctl.d.
if [ ! -e /etc/sysctl.d/99-cqlite-perf.conf ] || [ -n "${CQLITE_PERF_ALLOW_REAL_DROPIN:-}" ]; then
  ok "perf section: the suite never created the real /etc/sysctl.d/99-cqlite-perf.conf"
else
  # Pre-existing on a bootstrapped box is legitimate; only report if WE made it.
  # `-nt` against a file stamped at suite start — no GNU-only `stat -c %Y`.
  if [ /etc/sysctl.d/99-cqlite-perf.conf -nt "$suite_ref" ]; then
    bad "perf section: the suite wrote the REAL /etc/sysctl.d/99-cqlite-perf.conf"
  else
    ok "perf section: the real drop-in pre-dates this suite (not written by it)"
  fi
fi

echo
echo "PASS=$PASS FAIL=$FAIL"
[ "$FAIL" -eq 0 ]
