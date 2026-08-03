#!/usr/bin/env bash
# Regression test for the PERF PROFILING CAPABILITY BOOTSTRAP SECTION (issue #3249):
# the part of scripts/bootstrap-agent-machine.sh that installs
# /etc/sysctl.d/99-cqlite-perf.conf, applies it, READS THE RESULT BACK out of /proc, and
# only then reports a verdict — across the boxes a fleet actually has (no sudo binary,
# sudo needing a password, already root, no perf, Darwin, a container whose sysctl
# silently does nothing).
#
# The HELPER's own unit contract lives in the sibling
# scripts/tests/test_perf_capability.sh; the shared harness (identity/platform stubs,
# perf shims, host-safety asserts) lives in
# scripts/tests/lib/perf-capability-test-lib.sh. Both are wired into the gate's
# `tooling-tests` component.
#
# WHAT IT ASSERTS, beyond "the code is there": that the FUNCTIONAL verification is
# HONOURED and that NO run can print an unqualified "VERIFIED" it has not earned.
# `perf stat` exits 0 while printing `<not supported>`/`<not counted>`, a virtualised
# PMU reports a flat 0, `sysctl` exits 0 while the value does not take, and ROOT
# BYPASSES perf_event_paranoid entirely — so every negative case here drives one of
# those states and requires the run to say so, while still exiting 0 (bootstrap is the
# fleet provisioning entry point and may never hard-fail a box).
#
# HOST SAFETY. Nothing here touches the real /etc/sysctl.d or /proc: the test-only env
# seams stand in (and since review R4-3 test mode REFUSES to fall back to a production
# directory) and every privileged/mutating tool is a recording PATH shim. The final case
# asserts that mutation-freedom directly.
#
# Run standalone:   bash scripts/tests/test_perf_capability_bootstrap.sh
# Or via the gate:  scripts/agent-gate.sh runs it in the `tooling-tests` component.
set -uo pipefail

# shellcheck source=scripts/tests/lib/perf-capability-test-lib.sh
. "$(cd "$(dirname "$0")" && pwd)/lib/perf-capability-test-lib.sh"

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


# Nothing in this suite may have touched the REAL /etc/sysctl.d.
perf_test_assert_host_clean
perf_test_report
