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
# Wall-clock stamp used ONLY to attribute a pre-existing host file (the last case
# asks "did WE create /etc/sysctl.d/99-cqlite-perf.conf, or was the box already
# bootstrapped?"). Never a threshold on a measured duration. perf-gate-allow
SUITE_START=$(date +%s)
ok()  { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad() { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }

tmp=$(mktemp -d "${TMPDIR:-/tmp}/perf-cap-test.XXXXXX")
trap 'rm -rf "$tmp"' EXIT

# Global-state isolation, same posture as test_bootstrap_agent_machine.sh: the
# bootstrap runs below read/write git config and read board env, and this suite runs
# inside `tooling-tests` on the very box hosting a live delivery session.
export GIT_CONFIG_GLOBAL="$tmp/global-gitconfig"
export GIT_CONFIG_NOSYSTEM=1
: >"$GIT_CONFIG_GLOBAL"
unset CQLITE_PROJECT_NUMBER CQLITE_PROJECT_OWNER CQLITE_PROJECT_ACCOUNT PROJECT_TITLE
# A worker shell may export the seams themselves; a test must set exactly what it means.
unset CQLITE_PERF_PROC_DIR CQLITE_PERF_SYSCTL_DIR

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
if [ "$(CQLITE_PERF_SYSCTL_DIR=/etc/sysctl.d bash "$PERFLIB" --drop-in-path)" = /etc/sysctl.d/99-cqlite-perf.conf ]; then
  ok "perf-capability: drop-in path is /etc/sysctl.d/99-cqlite-perf.conf (survives reboot)"
else
  bad "perf-capability: unexpected drop-in path"
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
  CQLITE_PERF_PROC_DIR="$perf_proc" CQLITE_PERF_SYSCTL_DIR="$perf_sysctl_d" \
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
# It must instead PRINT the exact remedy — including the reboot-persistent file and
# the apply — plus the AC5 posture and the BPF caveat (#3217).
if printf '%s' "$check_out" | grep -q 'perf-capability.sh --drop-in | sudo tee .*99-cqlite-perf.conf' \
   && printf '%s' "$check_out" | grep -q 're-run with --yes'; then
  ok "perf section: prints the exact drop-in remedy line instead of running it"
else
  bad "perf section: no exact remedy line printed"
  printf '%s\n' "$check_out" | sed -n '/Perf profiling/,/^$/p'
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
  CQLITE_PERF_PROC_DIR="$proc_yes" CQLITE_PERF_SYSCTL_DIR="$sysctl_yes" \
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
  CQLITE_PERF_PROC_DIR="$proc_yes" CQLITE_PERF_SYSCTL_DIR="$sysctl_yes" \
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
  CQLITE_PERF_PROC_DIR="$proc_yes" CQLITE_PERF_SYSCTL_DIR="$sysctl_yes" \
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
  CQLITE_PERF_PROC_DIR="$proc_yes" CQLITE_PERF_SYSCTL_DIR="$sysctl_yes" \
  bash "$tmp/perf-root-yes/scripts/bootstrap-agent-machine.sh" --skip-smoke --yes 2>&1)
if printf '%s' "$zero_out" | grep -q 'perf capability NOT verified' \
   && ! printf '%s' "$zero_out" | grep -q 'perf capability VERIFIED'; then
  ok "perf section: a zero cycle count WARNs (never 'verified')"
else
  bad "perf section: a zero cycle count was accepted as verified"
  printf '%s\n' "$zero_out" | sed -n '/Perf profiling/,/^$/p'
fi
# 4c. A box with NO sudo at all: warn + the exact remedy, no write, still exit 0.
nosudo="$tmp/perf-nosudo"; mkdir -p "$nosudo"
for t in bash cat sed awk grep printf tr cut sort head tail wc env date mktemp uname id \
         diff timeout dirname basename ls rm mv cp mkdir touch git python3 hostname stat find; do
  s=$(command -v "$t" 2>/dev/null) && ln -sf "$s" "$nosudo/$t"
done
ln -sf "$perfbin/perf" "$nosudo/perf"
mkperfshim 5555555
nosudo_sysctl="$tmp/perf-nosudo.d"; mkdir -p "$nosudo_sysctl"
nosudo_out=$(PATH="$nosudo" HOME="$yes_home" CARGO_HOME="$yes_home/.cargo" \
  CQLITE_PERF_PROC_DIR="$proc_yes" CQLITE_PERF_SYSCTL_DIR="$nosudo_sysctl" \
  bash "$tmp/perf-root-yes/scripts/bootstrap-agent-machine.sh" --skip-smoke --yes 2>&1)
nosudo_rc=$?
if [ "$nosudo_rc" -eq 0 ] && [ -z "$(ls -A "$nosudo_sysctl")" ] \
   && printf '%s' "$nosudo_out" | grep -q 'no non-interactive root' \
   && printf '%s' "$nosudo_out" | grep -q 'sudo tee .*99-cqlite-perf.conf'; then
  ok "perf section: no-sudo box warns with the exact remedy, writes nothing, exits 0"
else
  bad "perf section: no-sudo box mishandled (rc=$nosudo_rc, dir='$(ls -A "$nosudo_sysctl")')"
  printf '%s\n' "$nosudo_out" | sed -n '/Perf profiling/,/^$/p'
fi
# 4d. Nothing in this whole suite may have touched the REAL /etc/sysctl.d.
if [ ! -e /etc/sysctl.d/99-cqlite-perf.conf ] || [ -n "${CQLITE_PERF_ALLOW_REAL_DROPIN:-}" ]; then
  ok "perf section: the suite never created the real /etc/sysctl.d/99-cqlite-perf.conf"
else
  # Pre-existing on a bootstrapped box is legitimate; only report if WE made it.
  if [ "$(stat -c %Y /etc/sysctl.d/99-cqlite-perf.conf 2>/dev/null || echo 0)" -gt "$SUITE_START" ]; then
    bad "perf section: the suite wrote the REAL /etc/sysctl.d/99-cqlite-perf.conf"
  else
    ok "perf section: the real drop-in pre-dates this suite (not written by it)"
  fi
fi

echo
echo "PASS=$PASS FAIL=$FAIL"
[ "$FAIL" -eq 0 ]
