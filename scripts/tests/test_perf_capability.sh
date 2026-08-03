#!/usr/bin/env bash
# Regression test for the PERF PROFILING CAPABILITY HELPER (issue #3249):
# scripts/perf-capability.sh — the free /proc token, the canonical
# /etc/sysctl.d/99-cqlite-perf.conf bytes, the byte-exact idempotency compare, the
# test-mode env guard, the functional `perf stat` verdict and the privilege-drop
# identity resolution.
#
# The BOOTSTRAP-side integration cases (install + apply + read-back + verdict, across
# root/no-sudo/Darwin/missing-perf boxes) live in the sibling
# scripts/tests/test_perf_capability_bootstrap.sh; the shared harness (identity/platform
# stubs, perf shims, host-safety asserts) lives in
# scripts/tests/lib/perf-capability-test-lib.sh. Both are wired into the gate's
# `tooling-tests` component.
#
# WHY THIS EXISTS. Agent/worker images ship kernel.perf_event_paranoid = 4 — ALL
# unprivileged perf use denied — and set it in no sysctl file, so a box is
# profileable only by accident and reverts on reboot. Two measurement cycles were
# lost to that, largely because the denial's help text ("access limited") reads
# like a CAPABILITY verdict when it is a PERMISSION verdict.
#
# WHAT IT ASSERTS, beyond "the code is there": that no path can reach a
# CAPABLE/VERIFIED verdict from an UNVALIDATED input. Every "unknown" — an unreadable
# /proc value, an unparseable cycle count, an unusable `id -u`, an inconsistent
# SUDO_USER, a missing test-mode sandbox — must resolve AWAY from the reassuring answer.
#
# HOST SAFETY. Nothing here touches the real /etc/sysctl.d or /proc: the test-only env
# seams stand in and every privileged/mutating tool is a recording PATH shim. The final
# case asserts that mutation-freedom directly.
#
# Run standalone:   bash scripts/tests/test_perf_capability.sh
# Or via the gate:  scripts/agent-gate.sh runs it in the `tooling-tests` component.
set -uo pipefail

# shellcheck source=scripts/tests/lib/perf-capability-test-lib.sh
. "$(cd "$(dirname "$0")" && pwd)/lib/perf-capability-test-lib.sh"

# --- 1. The shared helper: scripts/perf-capability.sh ------------------------------
# Agent images ship kernel.perf_event_paranoid = 4 (all unprivileged perf DENIED)
# and set it in no sysctl file, so a box is profileable only by accident and
# reverts on reboot. Bootstrap installs /etc/sysctl.d/99-cqlite-perf.conf and — the
# part that matters — VERIFIES the result instead of assuming it.
#
# Cases below cover the helper's whole contract and are written so NOTHING touches
# the host: two test-only env seams stand in for the real paths
# (CQLITE_PERF_PROC_DIR for /proc/sys/kernel, CQLITE_PERF_SYSCTL_DIR for
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
# ...and a value with INTERIOR whitespace must stay malformed. The read used to cut the
# value at its first space (`${v%%[[:space:]]*}`), so `0 1` became a perfectly
# capable-looking `0` — an unknown resolving to the GOOD case in the one function the
# gate's `perf=` token comes from (fail-open audit, #3249 review round 4). Surrounding
# whitespace is still trimmed, so a normal `-1\n` (or a CRLF file) reads fine.
check_token '0 1' 0 unknown
check_token '-1 junk' 0 unknown
check_token 0 '0 1' unknown
check_token '  -1  ' 0 ok
printf -- '-1\r\n' >"$perfproc/perf_event_paranoid"; printf '0\r\n' >"$perfproc/kptr_restrict"
if [ "$(CQLITE_PERF_PROC_DIR="$perfproc" bash "$PERFLIB" --token)" = ok ]; then
  ok "perf-capability: a CRLF /proc value still reads as ok (surrounding whitespace trimmed, not truncated)"
else
  bad "perf-capability: a CRLF /proc value was misread"
fi
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
# refusal, so test mode can never reach a real privileged tool. Every case below supplies
# BOTH sandbox seams, because under the marker they are MANDATORY (1c-iii) — the tool
# check is what each of these is about, and an unsandboxed run would never reach it.
realpriv="$tmp/realpriv"; mkdir -p "$realpriv"
for t in sudo sysctl; do
  printf '#!/usr/bin/env bash\nexit 0\n' >"$realpriv/$t"; chmod +x "$realpriv/$t"
done
seamed_proc="$tmp/seamed-proc"; mkdir -p "$seamed_proc"
seamed_d="$tmp/seamed-sysctl.d"; mkdir -p "$seamed_d"
guard2_out=$(PATH="$realpriv:$PATH" CQLITE_PERF_TEST_MODE=1 CQLITE_PERF_TEST_PRIV_DIR="$tmp/some-other-dir" \
  CQLITE_PERF_PROC_DIR="$seamed_proc" CQLITE_PERF_SYSCTL_DIR="$seamed_d" \
  bash -c '. "$1"; perf_capability_env_guard' _ "$PERFLIB" 2>&1); guard2_rc=$?
if [ "$guard2_rc" -ne 0 ] && printf '%s' "$guard2_out" | grep -q 'outside the declared shim dir'; then
  ok "perf-capability: test mode REFUSES when sudo resolves outside the declared shim dir"
else
  bad "perf-capability: test mode accepted an undeclared sudo (rc=$guard2_rc, out='$guard2_out')"
fi
# ...and `sysctl` is guarded as strictly as `sudo` (a real `sysctl --system` would
# reconfigure the HOST kernel, marker or not).
guard3_out=$(PATH="$realpriv:$PATH" CQLITE_PERF_TEST_MODE=1 CQLITE_PERF_TEST_PRIV_DIR="$tmp/only-sudo-here" \
  CQLITE_PERF_PROC_DIR="$seamed_proc" CQLITE_PERF_SYSCTL_DIR="$seamed_d" \
  bash -c '. "$1"; perf_capability_env_guard' _ "$PERFLIB" 2>&1)
if printf '%s' "$guard3_out" | grep -q 'sysctl resolves to\|sudo resolves to'; then
  ok "perf-capability: test mode guards BOTH sudo and sysctl against a real binary"
else
  bad "perf-capability: test mode did not name the offending privileged tool: '$guard3_out'"
fi
if PATH="$realpriv:$PATH" CQLITE_PERF_TEST_MODE=1 CQLITE_PERF_TEST_PRIV_DIR="$realpriv" \
     CQLITE_PERF_PROC_DIR="$seamed_proc" CQLITE_PERF_SYSCTL_DIR="$seamed_d" \
     bash -c '. "$1"; perf_capability_env_guard' _ "$PERFLIB" 2>/dev/null; then
  ok "perf-capability: test mode ACCEPTS a sudo inside the declared shim dir"
else
  bad "perf-capability: test mode rejected its own declared shim dir"
fi

# 1c-iii. TEST MODE HAS NO PRODUCTION FALLBACK (issue #3249 review R4-3). With the
#         marker set and a path seam MISSING, the resolvers used to fall back to the REAL
#         /etc/sysctl.d and /proc/sys/kernel — so a test-mode run could pass the env
#         guard (sudo/sysctl absent, or present as declared shims) and a later root
#         `--yes` run would `tee` the host's real drop-in. "Hermetic" may not depend on a
#         variable happening to be set: an absent or production-shaped seam is a loud
#         refusal, and the path/proc resolvers refuse with it.
noseam_out=$(env -u CQLITE_PERF_PROC_DIR -u CQLITE_PERF_SYSCTL_DIR CQLITE_PERF_TEST_MODE=1 \
  bash -c '. "$1"; perf_capability_env_guard' _ "$PERFLIB" 2>&1); noseam_rc=$?
if [ "$noseam_rc" -ne 0 ] \
   && printf '%s' "$noseam_out" | grep -q 'requires an explicit NON-PRODUCTION CQLITE_PERF_PROC_DIR' \
   && printf '%s' "$noseam_out" | grep -q 'requires an explicit NON-PRODUCTION CQLITE_PERF_SYSCTL_DIR' \
   && printf '%s' "$noseam_out" | grep -q 'NEVER falls back'; then
  ok "perf-capability: test mode with NO path seams REFUSES loudly and names BOTH missing sandbox dirs"
else
  bad "perf-capability: test mode without seams was allowed to act (rc=$noseam_rc, out='$noseam_out')"
fi
# A seam pointing AT production (or anywhere under /etc, /proc, /sys) is the same hole
# wearing a seam, and a RELATIVE path is not a sandbox either.
# Each rejection is asserted BY ITS REASON, not merely by a non-zero rc: this guard has a
# second refusal (a real sudo/sysctl on PATH) that would otherwise satisfy an rc-only
# check and let a seam-validation regression pass unnoticed.
guard_rejects_seam() { # guard_rejects_seam <which:PROC|SYSCTL> <proc-seam> <sysctl-seam>
  local out
  out=$(env CQLITE_PERF_TEST_MODE=1 CQLITE_PERF_TEST_PRIV_DIR="$realpriv" \
          CQLITE_PERF_PROC_DIR="$2" CQLITE_PERF_SYSCTL_DIR="$3" \
          bash -c '. "$1"; perf_capability_env_guard' _ "$PERFLIB" 2>&1) && return 1
  printf '%s' "$out" | grep -q "NON-PRODUCTION CQLITE_PERF_${1}_DIR"
}
for badseam in /etc/sysctl.d /etc/sysctl.d/sub /etc /proc /proc/sys/kernel /sys relative/dir ''; do
  guard_rejects_seam SYSCTL "$seamed_proc" "$badseam" || {
    bad "perf-capability: test mode did not reject the sysctl seam '$badseam' as non-production"; badseam_fail=1; }
  guard_rejects_seam PROC "$badseam" "$seamed_d" || {
    bad "perf-capability: test mode did not reject the proc seam '$badseam' as non-production"; badseam_fail=1; }
done
# ...and a seam that is a SYMLINK to production passes every TEXTUAL check while still
# landing a root `tee` in the real directory, so the seam itself may not be a symlink
# (fail-open audit, #3249 review round 4).
symseam="$tmp/symlink-to-production"; rm -f "$symseam"; ln -s /etc/sysctl.d "$symseam"
symproc="$tmp/symlink-to-proc"; rm -f "$symproc"; ln -s /proc/sys/kernel "$symproc"
guard_rejects_seam SYSCTL "$seamed_proc" "$symseam" || {
  bad "perf-capability: test mode ACCEPTED a sysctl seam that is a SYMLINK to /etc/sysctl.d"; badseam_fail=1; }
guard_rejects_seam PROC "$symproc" "$seamed_d" || {
  bad "perf-capability: test mode ACCEPTED a proc seam that is a SYMLINK to /proc/sys/kernel"; badseam_fail=1; }
# ...and the SPELLING is not the destination (issue #3249 review R5-1): `/tmp/../etc/sysctl.d`
# and `<symlink-to-/etc>/sysctl.d` pass every textual test above, and a root `--yes` run
# resolves BOTH to the production directory. Each escape so far was one more spelling of
# "somewhere else", so the write-side guard now judges the CANONICAL destination. An
# UNENTERABLE path resolves to nothing and is refused too — a write target must exist.
symanc="$tmp/symlinked-ancestor"; rm -f "$symanc"; ln -s /etc "$symanc"
for resolveseam in "/tmp/../etc/sysctl.d" "$symanc/sysctl.d" "$tmp/./nonexistent-sandbox.d" \
                   "$tmp/no-such-sandbox.d"; do
  guard_rejects_seam SYSCTL "$seamed_proc" "$resolveseam" || {
    bad "perf-capability: test mode ACCEPTED a sysctl seam that RESOLVES outside its sandbox: '$resolveseam'"; badseam_fail=1; }
done
guard_rejects_seam PROC "$symanc/sysctl.d" "$seamed_d" || {
  bad "perf-capability: test mode ACCEPTED a proc seam with a SYMLINKED ANCESTOR into /etc"; badseam_fail=1; }
[ -n "${badseam_fail:-}" ] || ok "perf-capability: test mode rejects an empty/relative/production-shaped/SYMLINKED seam AND one that merely RESOLVES into production (.. or a symlinked ancestor), on BOTH sandbox dirs, naming the offending seam"
# ...and the write TARGET is re-validated independently of the guard: --drop-in-path may
# never NAME a production file, because that string is what a root `tee` is pointed at.
for resolveseam in "/tmp/../etc/sysctl.d" "$symanc/sysctl.d"; do
  rp=$(env CQLITE_PERF_TEST_MODE=1 CQLITE_PERF_SYSCTL_DIR="$resolveseam" \
         bash "$PERFLIB" --drop-in-path 2>/dev/null); rp_rc=$?
  if [ "$rp_rc" -eq 0 ] || [ -n "$rp" ]; then
    bad "perf-capability: --drop-in-path named a write target from a seam resolving into production ('$resolveseam' -> '$rp')"
    dropinpath_fail=1
  fi
done
[ -n "${dropinpath_fail:-}" ] || ok "perf-capability: the drop-in write TARGET is refused (rc 1, empty) for a seam that resolves into production, independently of the env guard"
# ...and the refusal reaches the RESOLVERS, not only the guard: an unsandboxed test mode
# must not be able to name a production path at all (this is what the `tee` would use).
noseam_path=$(env -u CQLITE_PERF_SYSCTL_DIR CQLITE_PERF_TEST_MODE=1 bash "$PERFLIB" --drop-in-path 2>/dev/null)
noseam_path_rc=$?
noseam_tok=$(env -u CQLITE_PERF_PROC_DIR CQLITE_PERF_TEST_MODE=1 bash "$PERFLIB" --token 2>/dev/null)
if [ "$noseam_path_rc" -ne 0 ] && [ -z "$noseam_path" ] && [ "$noseam_tok" = absent ]; then
  ok "perf-capability: unsandboxed test mode resolves NO drop-in path (rc 1, empty) and reads NO /proc (token 'absent')"
else
  bad "perf-capability: unsandboxed test mode still resolved production paths (path rc=$noseam_path_rc '$noseam_path', token '$noseam_tok')"
fi

# 1d. The FUNCTIONAL verification is HONOURED, not merely attempted — the #3119
#      lesson (test_bootstrap_agent_machine.sh case 10) applied to this issue. `perf stat` exits 0 while
#      printing `<not supported>` / `<not counted>`, and a virtualised PMU can
#      report a flat 0, so an rc-only check is exactly the false green being fixed.
#      `mkperfshim` / `mkperfshim_raw` / `$perfbin` / `$PERFSHIM_LOG` come from
#      scripts/tests/lib/perf-capability-test-lib.sh (both suites drive the same shims).
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

# 1d-vi. THE COMPARE IS ACTUALLY BYTE-EXACT (issue #3249 review R4-4). `$( )` strips
#        EVERY trailing newline from its output, so comparing two command substitutions
#        judged a file missing its final newline — or carrying extra trailing blank lines
#        — as CURRENT, and it was never rewritten. That also made a documented claim
#        ("byte-exact") false. Both variants must be NOT current; the canonical bytes
#        must still be current, so the fix cannot be "always rewrite".
dropin_current_is() { # dropin_current_is <dir>  -> rc of perf_capability_dropin_current
  CQLITE_PERF_TEST_MODE=1 CQLITE_PERF_SYSCTL_DIR="$1" \
    bash -c '. "$1"; perf_capability_dropin_current' _ "$PERFLIB" 2>/dev/null
}
bytes_d="$tmp/perf-bytes.d"; mkdir -p "$bytes_d"
bytes_f="$bytes_d/99-cqlite-perf.conf"
bytes_fail=0
bash "$PERFLIB" --drop-in >"$bytes_f"
dropin_current_is "$bytes_d" || { bad "perf-capability: the CANONICAL drop-in bytes were judged NOT current"; bytes_fail=1; }
# variant 1: the final newline stripped (exactly what a `$(...) > file` remedy produces)
printf '%s' "$(bash "$PERFLIB" --drop-in)" >"$bytes_f"
if dropin_current_is "$bytes_d"; then
  bad "perf-capability: a drop-in MISSING its final newline was judged current (the compare is not byte-exact)"
  bytes_fail=1
fi
# variant 2: an extra trailing blank line (an editor, or a doubled remedy paste)
bash "$PERFLIB" --drop-in >"$bytes_f"; printf '\n' >>"$bytes_f"
if dropin_current_is "$bytes_d"; then
  bad "perf-capability: a drop-in with an EXTRA trailing blank line was judged current"
  bytes_fail=1
fi
# variant 3: same bytes, one line's trailing whitespace added — content, not just newlines
bash "$PERFLIB" --drop-in | sed 's/^kernel.kptr_restrict = 0$/kernel.kptr_restrict = 0 /' >"$bytes_f"
if dropin_current_is "$bytes_d"; then
  bad "perf-capability: a drop-in with altered trailing whitespace on a value line was judged current"
  bytes_fail=1
fi
# variant 4/5: THE NUL CASE (issue #3249 review R5-3). `read -d ''` stops at a NUL and
# returns SUCCESS, leaving only the bytes BEFORE it in the variable — so canonical content
# followed by a NUL and ARBITRARY trailing bytes compared EQUAL and the file was never
# rewritten. Both a bare trailing NUL and a NUL with junk after it must be NOT current.
bash "$PERFLIB" --drop-in >"$bytes_f"; printf '\0' >>"$bytes_f"
if dropin_current_is "$bytes_d"; then
  bad "perf-capability: canonical content + a trailing NUL was judged current"
  bytes_fail=1
fi
bash "$PERFLIB" --drop-in >"$bytes_f"; printf '\0kernel.perf_event_paranoid = 4\n' >>"$bytes_f"
if dropin_current_is "$bytes_d"; then
  bad "perf-capability: canonical content + NUL + arbitrary trailing bytes was judged current (the read stopped at the NUL and never saw them)"
  bytes_fail=1
fi
[ "$bytes_fail" -ne 0 ] || ok "perf-capability: the idempotency compare is BYTE-exact — a missing final newline, an extra blank line, altered trailing whitespace, a trailing NUL and a NUL followed by arbitrary bytes are all NOT current"

# 1d-vii. THE '99-' PREFIX IS LOAD-BEARING, and the drop-in says so in its own bytes.
#         This box ships /etc/sysctl.d/10-kernel-hardening.conf with
#         `kernel.kptr_restrict = 1`; our file only wins because "99-…" sorts after
#         "10-…". A future tidy-up to `cqlite-perf.conf` would silently hand kptr_restrict
#         back to the hardening drop-in at the next boot — the "silent revert" three
#         measurement reports recorded without ever naming a cause.
if printf '%s\n' "$dropin" | grep -q '99-' \
   && printf '%s\n' "$dropin" | grep -qi 'DO NOT RENAME' \
   && printf '%s\n' "$dropin" | grep -q '10-kernel-hardening.conf'; then
  ok "perf-capability: the drop-in header states that the '99-' prefix is load-bearing and NAMES 10-kernel-hardening.conf"
else
  bad "perf-capability: the drop-in header does not explain the 99- ordering / name the competing hardening file"
fi

# 1d-viii. NAME THE COMPETITOR. perf_capability_competing_files must find every OTHER
#          file in the sysctl.d dir that sets perf_event_paranoid/kptr_restrict, and rank
#          it by BASENAME order: one sorting AFTER ours is an actual override (applied
#          last, wins); one sorting before is harmless. Unrelated knobs and our own file
#          are not competitors.
comp_d="$tmp/perf-competing.d"; mkdir -p "$comp_d"
bash "$PERFLIB" --drop-in >"$comp_d/99-cqlite-perf.conf"
printf '# stock ubuntu hardening\nkernel.kptr_restrict = 1\n' >"$comp_d/10-kernel-hardening.conf"
printf 'kernel.perf_event_paranoid = 3\n'                     >"$comp_d/99-zzz-late.conf"
printf 'vm.swappiness = 1\n'                                  >"$comp_d/50-unrelated.conf"
printf '#kernel.kptr_restrict = 1\n'                          >"$comp_d/20-commented.conf"
comp_out=$(CQLITE_PERF_TEST_MODE=1 CQLITE_PERF_SYSCTL_DIR="$comp_d" \
  bash -c '. "$1"; perf_capability_competing_files' _ "$PERFLIB" 2>&1)
if printf '%s\n' "$comp_out" | grep -q "^earlier $comp_d/10-kernel-hardening.conf$" \
   && printf '%s\n' "$comp_out" | grep -q "^override $comp_d/99-zzz-late.conf$" \
   && ! printf '%s\n' "$comp_out" | grep -q '50-unrelated' \
   && ! printf '%s\n' "$comp_out" | grep -q '20-commented' \
   && ! printf '%s\n' "$comp_out" | grep -q '99-cqlite-perf.conf'; then
  ok "perf-capability: competing sysctl files are found and ranked (10-kernel-hardening=earlier, 99-zzz-late=override; unrelated/commented/own file ignored)"
else
  bad "perf-capability: competing-file detection wrong: '$comp_out'"
fi
nocomp_empty_d="$tmp/perf-nocomp-empty.d"; mkdir -p "$nocomp_empty_d"
if [ -z "$(CQLITE_PERF_TEST_MODE=1 CQLITE_PERF_SYSCTL_DIR="$nocomp_empty_d" \
             bash -c '. "$1"; perf_capability_competing_files' _ "$PERFLIB" 2>/dev/null)" ]; then
  ok "perf-capability: an empty sysctl.d directory yields no competitors (and no error output)"
else
  bad "perf-capability: an empty sysctl.d dir produced competitor output"
fi

# 1d-ix. THE SCAN COVERS THE WHOLE `sysctl --system` SEARCH PATH, WITH MASKING (issue #3249
#        review R5-4). `sysctl --system`/systemd-sysctl load /etc/sysctl.d, /run/sysctl.d,
#        /usr/local/lib/sysctl.d, /usr/lib/sysctl.d, /lib/sysctl.d and finally the FILE
#        /etc/sysctl.conf; scanning only the first meant a later-sorting file in one of the
#        others overrode our drop-in while the diagnostic reported NO competitor. Two
#        independent rules are asserted here:
#          MASKING  a basename supplied by a HIGHER-precedence directory makes the lower
#                   copy ignored entirely — and it masks even when the higher copy sets
#                   NOTHING, which is the subtle half.
#          ORDERING among the surviving files, basename order decides; /etc/sysctl.conf is
#                   applied after them all, so it gets the distinct `last` verdict.
sp_hi="$tmp/perf-sp-hi.d";   mkdir -p "$sp_hi"
sp_run="$tmp/perf-sp-run.d"; mkdir -p "$sp_run"
sp_lib="$tmp/perf-sp-lib.d"; mkdir -p "$sp_lib"
sp_conf_d="$tmp/perf-sp-conf"; mkdir -p "$sp_conf_d"
bash "$PERFLIB" --drop-in >"$sp_hi/99-cqlite-perf.conf"
printf 'kernel.kptr_restrict = 1\n'       >"$sp_hi/10-hardening.conf"
printf '# nothing set here at all\n'      >"$sp_hi/60-inert.conf"     # masks the copy below
printf 'kernel.perf_event_paranoid = 3\n' >"$sp_run/60-inert.conf"    # IGNORED (masked)
printf 'kernel.perf_event_paranoid = 3\n' >"$sp_run/99-zzz-run.conf"  # later-sorting: WINS
printf '%s\n' '-kernel/kptr_restrict = 1' >"$sp_lib/95-usrlib.conf"   # slash + `-` spelling
printf 'kernel.perf_event_paranoid = 2\n' >"$sp_conf_d/sysctl.conf"   # applied LAST of all
sp_out=$(CQLITE_PERF_TEST_MODE=1 CQLITE_PERF_SYSCTL_DIR="$sp_hi" \
  CQLITE_PERF_SYSCTL_EXTRA_DIRS="$sp_run:$sp_lib:$sp_conf_d/sysctl.conf" \
  bash -c '. "$1"; perf_capability_competing_files' _ "$PERFLIB" 2>&1)
if printf '%s\n' "$sp_out" | grep -q "^earlier $sp_hi/10-hardening.conf$" \
   && printf '%s\n' "$sp_out" | grep -q "^override $sp_run/99-zzz-run.conf$" \
   && printf '%s\n' "$sp_out" | grep -q "^earlier $sp_lib/95-usrlib.conf$" \
   && printf '%s\n' "$sp_out" | grep -q "^last $sp_conf_d/sysctl.conf$" \
   && ! printf '%s\n' "$sp_out" | grep -q "$sp_run/60-inert.conf"; then
  ok "perf-capability: competing files are found across the WHOLE search path, ranked by basename, /etc/sysctl.conf gets the applied-last verdict, and a masked same-basename copy is skipped"
else
  bad "perf-capability: search-path scan wrong: '$sp_out'"
fi
# ...and an EXTRA-DIRS entry that is not an absolute non-production path fails the whole
# scan CLOSED (rc 1, no output): a test-mode scan may never read the host's real /run or
# /usr/lib, and a bad entry is an UNKNOWN, not "no competitor".
for spbad in /etc/sysctl.d relative/dir "/tmp/../etc/sysctl.d"; do
  sp_bad_out=$(CQLITE_PERF_TEST_MODE=1 CQLITE_PERF_SYSCTL_DIR="$sp_hi" \
    CQLITE_PERF_SYSCTL_EXTRA_DIRS="$spbad" \
    bash -c '. "$1"; perf_capability_competing_files' _ "$PERFLIB" 2>/dev/null) && sp_bad_fail=1
  [ -z "$sp_bad_out" ] || sp_bad_fail=1
  [ -z "${sp_bad_fail:-}" ] || bad "perf-capability: a production/relative CQLITE_PERF_SYSCTL_EXTRA_DIRS entry '$spbad' did not fail the scan closed"
done
[ -n "${sp_bad_fail:-}" ] || ok "perf-capability: a production-shaped/relative/resolving-into-production extra search-path entry fails the scan CLOSED (rc 1, no output)"
# ...and an UNREADABLE directory is an UNKNOWN (rc 1), never "no competing file": this
# diagnostic exists to replace an unknown with a named file, so answering an unknown with
# the reassuring line would recreate the mystery it was written to end.
unreadable_d="$tmp/perf-unreadable.d"; mkdir -p "$unreadable_d"; chmod 000 "$unreadable_d"
if [ -r "$unreadable_d" ]; then
  # real root ignores the mode bits; the property under test is unobservable here
  ok "perf-capability: (skipped under real root) unreadable sysctl.d dir — mode bits do not apply"
elif CQLITE_PERF_TEST_MODE=1 CQLITE_PERF_SYSCTL_DIR="$unreadable_d" \
       bash -c '. "$1"; perf_capability_competing_files' _ "$PERFLIB" >/dev/null 2>&1; then
  bad "perf-capability: an UNREADABLE sysctl.d dir reported success (an unknown became 'no competitor')"
else
  ok "perf-capability: an UNREADABLE sysctl.d directory fails the scan (rc 1) instead of claiming no competitor"
fi
chmod 755 "$unreadable_d"

# 1e. THE IDENTITY DIMENSION, at the helper level (issue #3249 review R4-1/R4-2). Every
#     assert here is the same shape: an UNKNOWN or UNVERIFIABLE identity must NOT resolve
#     to the reassuring answer.
idbox="$tmp/perf-idbox"; mkdir -p "$idbox"
for t in bash cat awk printf tr cut sort head tail wc env command timeout; do
  s=$(command -v "$t" 2>/dev/null) && ln -sf "$s" "$idbox/$t"
done
mkperfshim 7171717
ln -sf "$perfbin/perf" "$idbox/perf"
# 1e-i. `id -u` UNAVAILABLE. The old `$(id -u 2>/dev/null || echo 1000)` invented an
#       unprivileged answer, so a ROOT run set PERF_UNPRIV_EVIDENCE=1 and printed a false
#       VERIFIED — the R3-1 defect reintroduced through the detector meant to prevent it.
noid_out=$(PATH="$idbox" bash "$PERFLIB" --verify-unpriv 2>&1); noid_rc=$?
if [ "$noid_rc" -ne 0 ] && printf '%s' "$noid_out" | grep -q 'identity=identity-unknown' \
   && ! printf '%s' "$noid_out" | grep -q 'self-unprivileged'; then
  ok "perf-capability: with NO usable 'id' the identity is 'identity-unknown' and --verify-unpriv FAILS (never assumed unprivileged)"
else
  bad "perf-capability: a missing 'id' was treated as unprivileged (rc=$noid_rc, '$noid_out')"
fi
# ...and the same for an `id` that EXISTS but fails, or prints something unparseable.
mkdir -p "$tmp/perf-idbad" && cp -a "$idbox"/. "$tmp/perf-idbad"/
printf '#!/usr/bin/env bash\nexit 7\n' >"$tmp/perf-idbad/id"; chmod +x "$tmp/perf-idbad/id"
mkdir -p "$tmp/perf-idjunk" && cp -a "$idbox"/. "$tmp/perf-idjunk"/
printf '#!/usr/bin/env bash\necho "uid=0(root) gid=0(root)"\n' >"$tmp/perf-idjunk/id"; chmod +x "$tmp/perf-idjunk/id"
idbad_out=$(PATH="$tmp/perf-idbad" bash "$PERFLIB" --verify-unpriv 2>&1); idbad_rc=$?
idjunk_out=$(PATH="$tmp/perf-idjunk" bash "$PERFLIB" --verify-unpriv 2>&1); idjunk_rc=$?
if [ "$idbad_rc" -ne 0 ] && printf '%s' "$idbad_out" | grep -q 'identity=identity-unknown' \
   && [ "$idjunk_rc" -ne 0 ] && printf '%s' "$idjunk_out" | grep -q 'identity=identity-unknown'; then
  ok "perf-capability: an 'id -u' that FAILS or prints unparseable output is 'identity-unknown', not unprivileged"
else
  bad "perf-capability: a broken 'id -u' was accepted (fail rc=$idbad_rc '$idbad_out'; junk rc=$idjunk_rc '$idjunk_out')"
fi
# 1e-ii. AN INCONSISTENT SUDO_USER MAY NOT BE TRUSTED. SUDO_UID and SUDO_USER are
#        independent env strings: with `SUDO_UID=1000 SUDO_USER=root` and no setpriv, a
#        name-based `runuser -u root` / `sudo -u root` would run the probe AS ROOT while
#        the state claimed a successful drop. The name must be dropped and the VALIDATED
#        NUMERIC uid used instead (sudo's documented `#<uid>` form).
mkdir -p "$tmp/perf-idroot" && cp -a "$idbox"/. "$tmp/perf-idroot"/
cat >"$tmp/perf-idroot/id" <<'EOF'
#!/usr/bin/env bash
# root shell; `root` resolves to uid/gid 0, `agentuser` to 1000/1000, nothing else exists
case "${1:-}" in
  -u) if [ -n "${2:-}" ]; then case "$2" in root) echo 0 ;; agentuser) echo 1000 ;; *) exit 1 ;; esac
      else echo 0; fi ;;
  -g) if [ -n "${2:-}" ]; then case "$2" in root) echo 0 ;; agentuser) echo 1000 ;; *) exit 1 ;; esac
      else echo 0; fi ;;
  *) echo 0 ;;
esac
EOF
chmod +x "$tmp/perf-idroot/id"
idroot_argv="$tmp/perf-idroot-argv.log"
# sudo/runuser shims: record the FULL argv (so the assertions can prove WHICH identity the
# probe was actually asked to run as), then strip their own options and exec the rest.
for t in sudo runuser; do
  cat >"$tmp/perf-idroot/$t" <<EOF
#!/usr/bin/env bash
echo "$t \$*" >>"$idroot_argv"
while [ "\${1:-}" = -n ]; do shift; done
[ "\${1:-}" = -u ] && shift 2
[ "\${1:-}" = -- ] && shift
exec "\$@"
EOF
  chmod +x "$tmp/perf-idroot/$t"
done
: >"$idroot_argv"
stale_out=$(PATH="$tmp/perf-idroot" SUDO_UID=1000 SUDO_GID=1000 SUDO_USER=root \
  bash "$PERFLIB" --verify-unpriv 2>&1); stale_rc=$?
if [ "$stale_rc" -eq 0 ] \
   && printf '%s' "$stale_out" | grep -q 'identity=dropped:sudo:uid=1000' \
   && grep -q '^sudo -n -u #1000 -- perf stat' "$idroot_argv" \
   && ! grep -q 'runuser' "$idroot_argv" \
   && ! grep -q -- '-u root' "$idroot_argv"; then
  ok "perf-capability: SUDO_USER=root with SUDO_UID=1000 is REJECTED as a name — the drop uses the validated numeric uid (sudo -u '#1000'), never 'runuser -u root'"
else
  bad "perf-capability: a stale SUDO_USER steered the drop (rc=$stale_rc, state='$stale_out', argv='$(cat "$idroot_argv")')"
fi
# ...while a CONSISTENT SUDO_USER (passwd says it IS that uid/gid) is still usable, so
# the fix is a validation rather than a blanket refusal that would lose `runuser`.
: >"$idroot_argv"
good_out=$(PATH="$tmp/perf-idroot" SUDO_UID=1000 SUDO_GID=1000 SUDO_USER=agentuser \
  bash "$PERFLIB" --verify-unpriv 2>&1); good_rc=$?
if [ "$good_rc" -eq 0 ] && printf '%s' "$good_out" | grep -q 'identity=dropped:runuser:agentuser' \
   && grep -q '^runuser -u agentuser -- perf stat' "$idroot_argv"; then
  ok "perf-capability: a SUDO_USER whose passwd uid/gid MATCH the validated numerics is still used for 'runuser -u <name>'"
else
  bad "perf-capability: a consistent SUDO_USER was not usable (rc=$good_rc, '$good_out', argv='$(cat "$idroot_argv")')"
fi
# ...and a name that is not shell-token safe can never enter the word-split prefix.
: >"$idroot_argv"
inj_out=$(PATH="$tmp/perf-idroot" SUDO_UID=1000 SUDO_GID=1000 SUDO_USER='agent user; touch /tmp/pwn' \
  bash "$PERFLIB" --verify-unpriv 2>&1)
if printf '%s' "$inj_out" | grep -q 'identity=dropped:sudo:uid=1000' \
   && ! grep -q 'agent user' "$idroot_argv" && [ ! -e /tmp/pwn ]; then
  ok "perf-capability: a SUDO_USER containing shell metacharacters/whitespace never reaches the command prefix"
else
  bad "perf-capability: an unsafe SUDO_USER reached the prefix ('$inj_out', argv='$(cat "$idroot_argv")')"
fi
# 1e-iii. A NEGATIVE or oversized SUDO_UID is not an identity either (`[ -1 -gt 0 ]` is
#         false, but a negative uid slipping into `setpriv --reuid=-1` would be worse than
#         a refusal), and with no `nobody` on the box the state must be
#         root-no-unprivileged-target — never a probe as root labelled a drop.
mkdir -p "$tmp/perf-idnotarget" && cp -a "$tmp/perf-idroot"/. "$tmp/perf-idnotarget"/
cat >"$tmp/perf-idnotarget/id" <<'EOF'
#!/usr/bin/env bash
case "${1:-}" in
  -u|-g) if [ -n "${2:-}" ]; then exit 1; else echo 0; fi ;;   # no account resolves
  *) echo 0 ;;
esac
EOF
chmod +x "$tmp/perf-idnotarget/id"
notgt_fail=0
for badsudo in -1 0 abc 99999999999999999999999 ''; do
  bs_out=$(PATH="$tmp/perf-idnotarget" SUDO_UID="$badsudo" SUDO_GID=1000 SUDO_USER=agentuser \
    bash "$PERFLIB" --verify-unpriv 2>&1); bs_rc=$?
  if [ "$bs_rc" -eq 0 ] || ! printf '%s' "$bs_out" | grep -q 'identity=root-no-unprivileged-target'; then
    bad "perf-capability: SUDO_UID='$badsudo' did not fall through to root-no-unprivileged-target (rc=$bs_rc, '$bs_out')"
    notgt_fail=1
  fi
done
[ "$notgt_fail" -ne 0 ] || ok "perf-capability: a negative/zero/malformed/oversized/empty SUDO_UID is no identity — root with no 'nobody' reports root-no-unprivileged-target and FAILS"

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

# 1x. THE HARNESS'S OWN `mktemp` GUARD, OBSERVED FIRING (issue #3249 review R8-3). The
#     shared lib derives EVERY path from `$tmp`, and these suites run WITHOUT `set -e`
#     inside the MANDATORY tooling-tests component, sometimes under a root identity — so
#     an unchecked `tmp=$(mktemp -d …)` was a host-damage defect, not a style nit: with
#     `tmp` empty the setup lines write /global-gitconfig, /perfbin, /perfshim.log and
#     /host-home, and `rm -f` /uname and /id. A guard nobody has WATCHED FIRE is not
#     evidence (a hardcoded `_PERF_STATE="ok"` once survived 118/118 asserts), so BOTH
#     failure shapes are driven here through a PATH shim — a non-zero `mktemp`, and an
#     rc-0 `mktemp` that prints an empty path — and the refusal must be named, non-zero,
#     must never reach the suite body, and must create no root-level path.
mtg="$tmp/mktemp-guard"; mkdir -p "$mtg/fail-rc" "$mtg/empty-out"
for t in bash dirname cat sed awk grep printf tr cut sort head tail wc env date chmod mkdir rm ln id uname; do
  s=$(command -v "$t" 2>/dev/null) || continue
  ln -sf "$s" "$mtg/fail-rc/$t"; ln -sf "$s" "$mtg/empty-out/$t"
done
printf '%s\n' '#!/usr/bin/env bash' 'exit 1'                >"$mtg/fail-rc/mktemp"
printf '%s\n' '#!/usr/bin/env bash' 'printf "\n"' 'exit 0'  >"$mtg/empty-out/mktemp"
chmod +x "$mtg/fail-rc/mktemp" "$mtg/empty-out/mktemp"
printf '%s\n' '. "$1"' 'echo REACHED-SUITE-BODY' >"$mtg/probe.sh"
# The root-level paths the harness would create with an empty `$tmp`. Compared
# BEFORE/AFTER rather than asserted absent, so a box that legitimately has one of these
# names cannot make the case fail for the wrong reason.
mtg_state() {
  local p
  for p in /global-gitconfig /perfbin /perfshim.log /host-home /brew /cargo /gh /roborev /uname /id; do
    [ -e "$p" ] && printf '%s\n' "$p"
  done
  return 0
}
mtg_before=$(mtg_state)
mtg_fail=0
for variant in fail-rc empty-out; do
  mtg_out=$(PATH="$mtg/$variant" bash "$mtg/probe.sh" \
    "$PERF_TEST_LIB_DIR/perf-capability-test-lib.sh" 2>&1); mtg_rc=$?
  case "$mtg_out" in
    *'REFUSING TO RUN (reason: unusable-temp-dir)'*) ;;
    *) bad "perf-lib: a $variant mktemp did not produce the NAMED refusal: '$mtg_out'"; mtg_fail=1 ;;
  esac
  [ "$mtg_rc" -ne 0 ] || { bad "perf-lib: a $variant mktemp still exited 0"; mtg_fail=1; }
  case "$mtg_out" in
    *REACHED-SUITE-BODY*) bad "perf-lib: a $variant mktemp did not stop the suite body"; mtg_fail=1 ;;
  esac
  if [ "$(mtg_state)" != "$mtg_before" ]; then
    bad "perf-lib: a $variant mktemp let the harness create a ROOT-LEVEL path ($(mtg_state))"; mtg_fail=1
  fi
done
[ "$mtg_fail" -ne 0 ] || ok "perf-capability-test-lib: an unusable 'mktemp -d' (non-zero rc, or rc 0 with an empty path) REFUSES with a named reason, exits non-zero, never reaches the suite body, and creates no root-level path"

# Nothing in this suite may have touched the REAL /etc/sysctl.d.
perf_test_assert_host_clean
perf_test_report
