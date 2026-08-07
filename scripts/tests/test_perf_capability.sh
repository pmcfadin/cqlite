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
   && printf '%s' "$noseam_out" | grep -q 'requires CQLITE_PERF_PROC_DIR INSIDE the declared sandbox' \
   && printf '%s' "$noseam_out" | grep -q 'requires CQLITE_PERF_SYSCTL_DIR INSIDE the declared sandbox' \
   && printf '%s' "$noseam_out" | grep -q 'NEVER falls back'; then
  ok "perf-capability: test mode with NO path seams REFUSES loudly and names BOTH missing sandbox dirs"
else
  bad "perf-capability: test mode without seams was allowed to act (rc=$noseam_rc, out='$noseam_out')"
fi
# A seam pointing AT production (or anywhere under /etc, /proc, /sys) is refused because it
# is not inside the sandbox — no forbidden name is consulted — and a RELATIVE path is not a
# sandbox path either. Each rejection is asserted BY ITS REASON, not merely by a non-zero rc:
# this guard has a second refusal (a real sudo/sysctl on PATH) that would otherwise satisfy
# an rc-only check and let a containment regression pass unnoticed.
guard_rejects_seam() { # guard_rejects_seam <which:PROC|SYSCTL> <proc-seam> <sysctl-seam>
  local out
  out=$(env CQLITE_PERF_TEST_MODE=1 CQLITE_PERF_TEST_PRIV_DIR="$realpriv" \
          CQLITE_PERF_PROC_DIR="$2" CQLITE_PERF_SYSCTL_DIR="$3" \
          bash -c '. "$1"; perf_capability_env_guard' _ "$PERFLIB" 2>&1) && return 1
  printf '%s' "$out" | grep -q "CQLITE_PERF_${1}_DIR INSIDE the declared sandbox"
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
# ...and the SPELLING is not the destination. `/tmp/../etc/sysctl.d`, `<symlink-to-/etc>/…`
# and — the R6-1 escape — `//etc/sysctl.d` (POSIX leaves two leading slashes
# implementation-defined and `pwd -P` may PRESERVE them, while on Linux `//etc` IS `/etc`)
# each passed the textual checks of an earlier round. There is no per-spelling check any
# more: containment refuses all of them, plus every future spelling, for the SAME reason.
# An UNENTERABLE path resolves to nothing and is refused too — a write target must exist.
symanc="$tmp/symlinked-ancestor"; rm -f "$symanc"; ln -s /etc "$symanc"
symout="$tmp/symlink-out-of-sandbox"; rm -f "$symout"; ln -s /tmp "$symout"
for resolveseam in "/tmp/../etc/sysctl.d" "$symanc/sysctl.d" "$tmp/./nonexistent-sandbox.d" \
                   "$tmp/no-such-sandbox.d" "//etc/sysctl.d" "//etc/sysctl.d/sub" \
                   "$symout" "$tmp"; do
  guard_rejects_seam SYSCTL "$seamed_proc" "$resolveseam" || {
    bad "perf-capability: test mode ACCEPTED a sysctl seam that is not strictly inside its sandbox: '$resolveseam'"; badseam_fail=1; }
done
for resolveseam in "$symanc/sysctl.d" "//proc/sys/kernel" "$symout"; do
  guard_rejects_seam PROC "$resolveseam" "$seamed_d" || {
    bad "perf-capability: test mode ACCEPTED a proc seam outside its sandbox: '$resolveseam'"; badseam_fail=1; }
done
[ -n "${badseam_fail:-}" ] || ok "perf-capability: test mode rejects an empty/relative/production-shaped/SYMLINKED seam, one that RESOLVES out of the sandbox (.., a symlinked ancestor, a symlink to /tmp), the '//etc' double-slash spelling, a sibling-prefix path and the sandbox ROOT itself — on BOTH sandbox dirs, naming the offending seam"
# 1c-iii-b. THE CONTAINMENT BOUNDARY AND THE SANDBOX ROOT ITSELF (issue #3249 review
# (full rationale: fleet-runbook.md, perf seam containment, 1c-iii-b-the-containment-boundary-and-the-sand)
sbx="$tmp/sandbox"; mkdir -p "$sbx/inside" "$sbx/inside-proc"; : >"$sbx/.cqlite-perf-sandbox"
mkdir -p "${sbx}evil"                       # the sibling whose NAME starts with the root's
nostamp="$tmp/unstamped-root"; mkdir -p "$nostamp/inside"
# The shim dir lives INSIDE the root each call declares. It has to: since #3261 AC4 a privileged
# tool must RESOLVE beneath the proven sandbox root, so a shim dir outside it is refused on its own
# merits — which would decide these cases for the wrong reason (they are about path containment).
sbxpriv="$sbx/priv"; mkdir -p "$sbxpriv"
for t in sudo sysctl; do
  printf '#!/usr/bin/env bash\nexit 0\n' >"$sbxpriv/$t"; chmod +x "$sbxpriv/$t"
done
guard_with_root() { # guard_with_root <sandbox-root> <proc-seam> <sysctl-seam> -> rc + stderr
  # The shim dir FIRST on PATH and declared as such: the guard's OTHER refusals (a real
  # sudo/sysctl reachable, an unresolvable privileged tool) must not be what decides these cases.
  env PATH="$1/priv:$PATH" CQLITE_PERF_TEST_MODE=1 CQLITE_PERF_TEST_PRIV_DIR="$1/priv" \
    CQLITE_PERF_TEST_SANDBOX="$1" CQLITE_PERF_PROC_DIR="$2" CQLITE_PERF_SYSCTL_DIR="$3" \
    bash -c '. "$1"; perf_capability_env_guard' _ "$PERFLIB" 2>&1
}
bnd_fail=0
guard_with_root "$sbx" "$sbx/inside-proc" "$sbx/inside" >/dev/null 2>&1 \
  || { bad "perf-capability: a seam genuinely INSIDE the declared sandbox was refused (the guard is vacuous)"; bnd_fail=1; }
bnd_out=$(guard_with_root "$sbx" "$sbx/inside-proc" "${sbx}evil") && bnd_fail=1
printf '%s' "$bnd_out" | grep -q 'CQLITE_PERF_SYSCTL_DIR INSIDE the declared sandbox' || bnd_fail=1
[ "$bnd_fail" -eq 0 ] || bad "perf-capability: '${sbx}evil' was treated as inside '$sbx' (prefix match without a / boundary), or a contained seam was refused"
[ "$bnd_fail" -ne 0 ] || ok "perf-capability: containment is boundary-exact — a seam inside the declared sandbox is ACCEPTED while the sibling '<root>evil' is REFUSED by name"
root_fail=0
for badroot in '' relative/sandbox "//$tmp" "$tmp/no-such-root" "$nostamp"; do
  ro=$(guard_with_root "$badroot" "$sbx/inside-proc" "$sbx/inside") && root_fail=1
  printf '%s' "$ro" | grep -q 'requires CQLITE_PERF_TEST_SANDBOX' || root_fail=1
  [ "$root_fail" -eq 0 ] || { bad "perf-capability: sandbox root '$badroot' was accepted (or refused without naming CQLITE_PERF_TEST_SANDBOX)"; break; }
done
[ "$root_fail" -ne 0 ] || ok "perf-capability: the sandbox ROOT must prove itself — unset/relative/'//'-spelled/absent/UNSTAMPED are refusals naming CQLITE_PERF_TEST_SANDBOX (so a stray CQLITE_PERF_TEST_SANDBOX=/etc cannot make containment vacuous)"
# ...and the FORK-FREE read path applies the same containment: a `//`-spelled or
# out-of-sandbox proc seam reads NOTHING (token `absent`), never the host's real /proc —
# whose paranoid/kptr values would otherwise show up as ok/paranoid-N/kptr-restricted here.
read_fail=0
for badproc in "//proc/sys/kernel" "$symanc/sysctl.d" "/proc/sys/kernel" "${sbx}evil"; do
  rt=$(env CQLITE_PERF_TEST_MODE=1 CQLITE_PERF_TEST_SANDBOX="$sbx" \
         CQLITE_PERF_PROC_DIR="$badproc" bash "$PERFLIB" --token 2>/dev/null)
  [ "$rt" = absent ] || { bad "perf-capability: the read path used an out-of-sandbox proc seam '$badproc' (token '$rt', expected 'absent')"; read_fail=1; }
done
[ "$read_fail" -ne 0 ] || ok "perf-capability: the fork-free READ path refuses an out-of-sandbox proc seam (including the '//proc' spelling) — token 'absent', the real /proc never read"
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
# ...and an EXTRA-DIRS entry that is not provably inside the sandbox fails the whole scan
# CLOSED (rc 1, no output): a test-mode scan may never read the host's real /run or /usr/lib,
# and a bad entry is an UNKNOWN, not "no competitor". THIS ENTRY POINT IS R6-2: it used the
# textual validator while the write path canonicalized, so a SYMLINKED ANCESTOR (and the
# `//etc` spelling) could point a "sandboxed" scan at the host's real configuration. It now
# goes through the same resolving gate — dirs and the `sysctl.conf` FILE entry alike.
for spbad in /etc/sysctl.d relative/dir "/tmp/../etc/sysctl.d" "//etc/sysctl.d" \
             "$symanc/sysctl.d" "$symanc/sysctl.conf" "$symout" "$tmp/../etc/sysctl.d"; do
  sp_this=0
  sp_bad_out=$(CQLITE_PERF_TEST_MODE=1 CQLITE_PERF_SYSCTL_DIR="$sp_hi" \
    CQLITE_PERF_SYSCTL_EXTRA_DIRS="$spbad" \
    bash -c '. "$1"; perf_capability_competing_files' _ "$PERFLIB" 2>/dev/null) && sp_this=1
  # No output at all, and in particular NOT ONE host path: this is the "no host file read"
  # half of the property, so it is asserted rather than inferred from the rc.
  [ -z "$sp_bad_out" ] || sp_this=1
  [ "$sp_this" -eq 0 ] || { bad "perf-capability: CQLITE_PERF_SYSCTL_EXTRA_DIRS entry '$spbad' did not fail the scan closed (out='$sp_bad_out')"; sp_bad_fail=1; }
done
[ -n "${sp_bad_fail:-}" ] || ok "perf-capability: an extra search-path entry outside the sandbox — production-shaped, relative, '//etc'-spelled, or reached through a SYMLINKED ANCESTOR (dir or sysctl.conf FILE) — fails the scan CLOSED (rc 1, no output, no host path named)"
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
# ...and the same rule PER FILE, with the two shapes distinguished (issue #3249 review
# R8-4). `for f in "$dir"/*.conf` leaves the PATTERN in $f when nothing matches, so one
# `[ -f ] && [ -r ] || continue` conflated "this directory holds no .conf" (genuinely no
# competitor) with "this .conf exists but I cannot read it" (an UNKNOWN — and a
# privileged `sysctl --system` CAN read and apply it). The second must fail the scan.
unrf_d="$tmp/perf-unreadable-file.d"; mkdir -p "$unrf_d"
printf 'kernel.perf_event_paranoid = 3\n' >"$unrf_d/10-secret.conf"; chmod 000 "$unrf_d/10-secret.conf"
noglob_d="$tmp/perf-noglob.d"; mkdir -p "$noglob_d"
printf 'not a sysctl file\n' >"$noglob_d/README.txt"   # readable dir, glob matches NOTHING
noglob_out=$(CQLITE_PERF_TEST_MODE=1 CQLITE_PERF_SYSCTL_DIR="$noglob_d" \
  bash -c '. "$1"; perf_capability_competing_files' _ "$PERFLIB" 2>&1); noglob_rc=$?
if [ -r "$unrf_d/10-secret.conf" ]; then
  # real root ignores the mode bits; the unreadable-FILE half is unobservable here
  ok "perf-capability: (skipped under real root) unreadable competing .conf — mode bits do not apply"
else
  unrf_out=$(CQLITE_PERF_TEST_MODE=1 CQLITE_PERF_SYSCTL_DIR="$unrf_d" \
    bash -c '. "$1"; perf_capability_competing_files' _ "$PERFLIB" 2>&1); unrf_rc=$?
  if [ "$unrf_rc" -ne 0 ] && printf '%s\n' "$unrf_out" | grep -q "could not scan .*10-secret.conf" \
     && ! printf '%s\n' "$unrf_out" | grep -qE '^(earlier|override|last) '; then
    ok "perf-capability: an EXISTING but UNREADABLE competing .conf fails the scan (rc 1, 'could not scan' naming the file) instead of being silently skipped into a clean bill"
  else
    bad "perf-capability: an unreadable competing .conf was skipped silently (rc=$unrf_rc, '$unrf_out')"
  fi
fi
if [ "$noglob_rc" -eq 0 ] && [ -z "$noglob_out" ]; then
  ok "perf-capability: a readable directory whose *.conf glob matches NOTHING is no competitor (rc 0, no output) — the unmatched glob is not treated as an unreadable file"
else
  bad "perf-capability: an unmatched *.conf glob did not scan clean (rc=$noglob_rc, '$noglob_out')"
fi
chmod 644 "$unrf_d/10-secret.conf"

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

# 1f. THE GATE IS SINGULAR AND UNSKIPPABLE — a STRUCTURAL audit (issue #3249 review R6-2).
# (full rationale: fleet-runbook.md, perf seam containment, 1f-the-gate-is-singular-and-unskippable-a-stru)
seam_audit=$(awk \
  -v seamre='[$][{]?CQLITE_PERF_(PROC_DIR|SYSCTL_DIR|SYSCTL_EXTRA_DIRS|TEST_SANDBOX|TEST_PRIV_DIR)' \
  -v gatere='(^|[[:space:];&|(])perf_capability_(sandbox_[a-z_]*|path_within)([[:space:]]|\\)|$)' '
  # TWO representations, because the two matches want different things (roborev round 6, Low).
  # (full rationale: fleet-runbook.md, perf seam containment, two-representations-because-the-two-matches-wa)
  { code = $0
    sub(/[[:space:]]*#.*$/, "", code)
    codeq = $0
    gsub(/"[^"]*"/, " ", codeq)
    gsub(/\047[^\047]*\047/, " ", codeq)
    sub(/[[:space:]]*#.*$/, "", codeq) }
  /^[a-z_][a-z_0-9]*\(\)[[:space:]]*\{/ {
    fn = $1; sub(/\(\)$/, "", fn); seam = 0; gate = 0
    if (code ~ seamre) seam = 1
    if (codeq ~ gatere) gate = 1
    if ($0 ~ /\}[[:space:]]*$/) { printf "%s %d %d\n", fn, seam, gate; inb = 0 }
    else inb = 1
    next
  }
  inb && /^\}/ { printf "%s %d %d\n", fn, seam, gate; inb = 0; next }
  inb {
    if (code ~ seamre) seam = 1
    if (codeq ~ gatere) gate = 1
  }
' "$PERFLIB")
# The ONLY function allowed to read a seam without the gate, and why: it asks whether a seam
# was handed to us AT ALL (for the marker-less refusal) and never uses the value as a path.
# The containment family's own root reader is the gate, so it is named here too.
seam_audit_allow=' perf_capability_seam_set perf_capability_sandbox_root_into '
seam_consumers=0; seam_ungated=''
while read -r fn seam gate; do
  [ -n "$fn" ] || continue
  [ "$seam" = 1 ] || continue
  seam_consumers=$((seam_consumers + 1))
  case "$seam_audit_allow" in *" $fn "*) continue ;; esac
  [ "$gate" = 1 ] || seam_ungated="$seam_ungated $fn"
done <<EOF
$seam_audit
EOF
# 5 is the count at the time of writing (proc_dir_into, sysctl_dir, sysctl_search_path,
# test_seams_ok, env_guard) plus the two allowlisted readers; the assert is a FLOOR, so adding
# a consumer is fine and losing the parse is not.
if [ "$seam_consumers" -ge 6 ] && [ -z "$seam_ungated" ] \
   && printf '%s\n' "$seam_audit" | grep -q '^perf_capability_sysctl_search_path 1 1$' \
   && printf '%s\n' "$seam_audit" | grep -q '^perf_capability_proc_dir_into 1 1$' \
   && printf '%s\n' "$seam_audit" | grep -q '^perf_capability_env_guard 1 1$'; then
  ok "perf-capability: STRUCTURAL — all $seam_consumers seam-dereferencing functions (CQLITE_PERF_TEST_PRIV_DIR included) route through the single containment gate (only the documented presence-check + the root reader are allowlisted), so a new entry point cannot silently skip it"
else
  bad "perf-capability: STRUCTURAL audit failed — seam consumers found: $seam_consumers; UNGATED:${seam_ungated:- none}"
  printf '%s\n' "$seam_audit"
fi
# 1f-ii. THE SAME AUDIT FOR THE BINARIES THE GUARD AUTHORIZES (issue #3261 AC4). AC4 was the
# (full rationale: fleet-runbook.md, perf seam containment, 1f-ii-the-same-audit-for-the-binaries-the-guar)
priv_audit=$(awk \
  -v privre='(for [a-z_]+ in (sudo|sysctl)|command -v .*(sudo|sysctl))' \
  -v gatere='(^|[[:space:];&|(])perf_capability_(sandbox_[a-z_]*|path_within)([[:space:]]|\\)|$)' '
  # TWO representations, because the two matches want different things (roborev round 6, Low).
  # (full rationale: fleet-runbook.md, perf seam containment, two-representations-because-the-two-matches-wa)
  { code = $0
    sub(/[[:space:]]*#.*$/, "", code)
    codeq = $0
    gsub(/"[^"]*"/, " ", codeq)
    gsub(/\047[^\047]*\047/, " ", codeq)
    sub(/[[:space:]]*#.*$/, "", codeq) }
  /^[a-z_][a-z_0-9]*\(\)[[:space:]]*\{/ {
    fn = $1; sub(/\(\)$/, "", fn); priv = 0; gate = 0
    if (code ~ privre) priv = 1
    if (codeq ~ gatere) gate = 1
    if ($0 ~ /\}[[:space:]]*$/) { printf "%s %d %d\n", fn, priv, gate; inb = 0 }
    else inb = 1
    next
  }
  inb && /^\}/ { printf "%s %d %d\n", fn, priv, gate; inb = 0; next }
  inb {
    if (code ~ privre) priv = 1
    if (codeq ~ gatere) gate = 1
  }
' "$PERFLIB")
# The ONE function allowed to resolve a privileged tool without the containment gate, and why:
# perf_capability_drop_prefix_into resolves setpriv/runuser/sudo to DE-escalate (run the `perf
# stat` probe as a LESS privileged identity). It cannot grant privilege, and in test mode
# perf_capability_env_guard has already refused if any real sudo/sysctl was reachable at all —
# so its inputs are already contained by the time it runs. Joining this list is a visible act.
priv_audit_allow=' perf_capability_drop_prefix_into '
priv_consumers=0; priv_ungated=''
while read -r fn priv gate; do
  [ -n "$fn" ] || continue
  [ "$priv" = 1 ] || continue
  priv_consumers=$((priv_consumers + 1))
  case "$priv_audit_allow" in *" $fn "*) continue ;; esac
  [ "$gate" = 1 ] || priv_ungated="$priv_ungated $fn"
done <<EOF
$priv_audit
EOF
if [ "$priv_consumers" -ge 2 ] && [ -z "$priv_ungated" ] \
   && printf '%s\n' "$priv_audit" | grep -q '^perf_capability_env_guard 1 1$'; then
  ok "perf-capability: STRUCTURAL — all $priv_consumers functions that RESOLVE a privileged tool route through the containment gate (only the documented de-escalation prefix builder is allowlisted), so the EXECUTABLES the guard authorizes are validated by destination too (#3261 AC4)"
else
  bad "perf-capability: STRUCTURAL privilege-shim audit failed — resolvers found: $priv_consumers; UNGATED:${priv_ungated:- none}"
  printf '%s\n' "$priv_audit"
fi

# ---- 1g. #3261: A NAME IS NOT A DESTINATION — the four remaining escapes ------------------
# Positive containment closed the PATH SPELLINGS. These four are what containment of a
# spelling still does not buy, and each is asserted BY ITS OWN OBSERVABLE CONSEQUENCE (a
# followed write, a fabricated /proc verdict, a refused legitimate file, a real privileged
# tool), never by an rc alone — this guard has several refusals and an rc-only check would let
# the wrong one satisfy the case.

# 1g-i. AC1 (High) — DIRECTORY containment is not WRITE-TARGET containment. `tee <path>` opens
#       O_WRONLY|O_CREAT|O_TRUNC and FOLLOWS a symlink, so a symlink at the managed basename
#       inside a perfectly-contained directory pointed the privileged write at the LINK'S
#       TARGET — anywhere on the box. A contained directory says nothing about where its
#       entries point. Two independent requirements, both asserted:
#         * anything that merely NAMES the write target REFUSES (rc 1, empty, loud);
#         * the WRITE ITSELF replaces the directory ENTRY (rename), so a symlink planted in
#           the window between the check and the write is replaced, not written through.
# (rationale condensed; full reasoning in the commit history for #3261.)
if perf_install_supported; then
  wt_d="$tmp/wt-sandbox.d"; mkdir -p "$wt_d"
  wt_outside="$tmp/wt-outside-target"; printf 'PRECIOUS-HOST-FILE\n' >"$wt_outside"
  wt_before=$(cat "$wt_outside")
  rm -f "$wt_d/99-cqlite-perf.conf"; ln -s "$wt_outside" "$wt_d/99-cqlite-perf.conf"
  wt_path=$(env CQLITE_PERF_SYSCTL_DIR="$wt_d" bash "$PERFLIB" --drop-in-path 2>/dev/null); wt_rc=$?
  wt_err=$(env CQLITE_PERF_SYSCTL_DIR="$wt_d" bash "$PERFLIB" --drop-in-path 2>&1 >/dev/null)
  if [ "$wt_rc" -ne 0 ] && [ -z "$wt_path" ] \
     && printf '%s' "$wt_err" | grep -qi 'symlink' \
     && [ -L "$wt_d/99-cqlite-perf.conf" ] && [ "$(cat "$wt_outside")" = "$wt_before" ]; then
    ok "perf-capability: the drop-in WRITE TARGET is refused when the managed basename is itself a SYMLINK (rc 1, empty, named) — a contained directory does not license writing through its entries (#3261 AC1)"
  else
    bad "perf-capability: a SYMLINKED write target was NAMED for a privileged tee (rc=$wt_rc, path='$wt_path', err='$wt_err')"
  fi
  # ...and the CONTENTS read that decides idempotency may not follow it either: a symlink whose
  # TARGET happens to hold the canonical bytes must not report "already current" (that would
  # leave the host file in place and claim success).
  printf '%s\n' "$(bash "$PERFLIB" --drop-in)" >"$wt_outside"
  if env CQLITE_PERF_SYSCTL_DIR="$wt_d" bash -c '. "$1"; perf_capability_dropin_current' _ "$PERFLIB" 2>/dev/null; then
    bad "perf-capability: dropin_current followed a SYMLINK and reported the drop-in 'already current' from a file outside the managed name"
  else
    ok "perf-capability: dropin_current does NOT follow a symlinked managed name, even when the link's TARGET holds byte-identical canonical content (#3261 AC1)"
  fi
  # ...and the WRITE replaces the ENTRY. The refusal above is a check with a TOCTOU window; the
  # rename has none. After it, the managed name is a REGULAR file holding the canonical bytes,
  # the outside target is byte-identical to before, and no temp entry is left behind.
  wt_outside_bytes=$(cat "$wt_outside")
  wt_ins=$(env CQLITE_PERF_SYSCTL_DIR="$wt_d" \
    bash -c '. "$1"; perf_capability_dropin_install' _ "$PERFLIB" 2>&1); wt_ins_rc=$?
  wt_leftover=$(ls -A "$wt_d" | grep -v '^99-cqlite-perf\.conf$' || true)
  if [ "$wt_ins_rc" -eq 0 ] && [ ! -L "$wt_d/99-cqlite-perf.conf" ] && [ -f "$wt_d/99-cqlite-perf.conf" ] \
     && [ "$(cat "$wt_outside")" = "$wt_outside_bytes" ] && [ -z "$wt_leftover" ] \
     && env CQLITE_PERF_SYSCTL_DIR="$wt_d" bash -c '. "$1"; perf_capability_dropin_current' _ "$PERFLIB"; then
    ok "perf-capability: the drop-in write REPLACES the directory entry (temp + rename), so a pre-existing symlink at the managed name is replaced and its outside target is untouched — and no temp entry is left behind (#3261 AC1)"
  else
    bad "perf-capability: the atomic drop-in install did not replace a symlinked entry (rc=$wt_ins_rc, out='$wt_ins', link=$([ -L "$wt_d/99-cqlite-perf.conf" ] && echo yes || echo no), leftover='$wt_leftover', outside-changed=$([ "$(cat "$wt_outside")" = "$wt_outside_bytes" ] && echo no || echo YES))"
  fi
  # ...and the STAGING entry is UNPREDICTABLE, created by `mktemp` (roborev finding 1 on #3261 — the
  # NINTH escape, same shape as the other eight: a NAME trusted instead of a DESTINATION). A fixed
  # staging path that is checked, cleared and only THEN opened by a privileged `tee` is a TOCTOU
  # window: anyone who can create entries in the directory re-plants that KNOWN name as a symlink
  # between the verify and the open, and root follows it. Two asserts, because neither alone is
  # enough — a behavioural one (the previously-predictable name is planted as a symlink at a victim
  # file and must be left strictly alone) and a structural one (unpredictability is a property of the
  # NAME, which is gone by the time the write succeeds, so the source is the only place to see it).
  wt_bait="$tmp/wt-staging-bait"; printf 'BAIT-MUST-NOT-BE-WRITTEN\n' >"$wt_bait"
  wt_bait_before=$(cat "$wt_bait")
  rm -f "$wt_d/.99-cqlite-perf.conf.new"; ln -s "$wt_bait" "$wt_d/.99-cqlite-perf.conf.new"
  rm -f "$wt_d/99-cqlite-perf.conf"
  wt_st=$(env CQLITE_PERF_SYSCTL_DIR="$wt_d" \
    bash -c '. "$1"; perf_capability_dropin_install' _ "$PERFLIB" 2>&1); wt_st_rc=$?
  if [ "$wt_st_rc" -eq 0 ] && [ "$(cat "$wt_bait")" = "$wt_bait_before" ] \
     && [ -L "$wt_d/.99-cqlite-perf.conf.new" ] \
     && [ -f "$wt_d/99-cqlite-perf.conf" ] && [ ! -L "$wt_d/99-cqlite-perf.conf" ]; then
    ok "perf-capability: the drop-in staging entry does NOT reuse the previously-predictable name — a symlink planted there is left untouched and its target is byte-unchanged, while the managed file is still written (#3261 roborev-1 TOCTOU)"
  else
    bad "perf-capability: the install wrote through a PREDICTABLE staging name (rc=$wt_st_rc, bait-changed=$([ "$(cat "$wt_bait")" = "$wt_bait_before" ] && echo no || echo YES), out='$wt_st')"
  fi
  rm -f "$wt_d/.99-cqlite-perf.conf.new"
  # ...structurally: the staging entry is created by `mktemp` with a random-suffix template, no
  # (rationale: fleet-runbook.md, perf seam containment, structurally-the-staging-entry-is-created-b)
  wt_body=$(awk '/^perf_capability_dropin_install\(\)/{f=1} f{print} f&&/^\}/{exit}' "$PERFLIB")
  wt_privcalls=$(printf '%s\n' "$wt_body" | grep -c '"\$@"')
  wt_struct_fail=''
  printf '%s\n' "$wt_body" | grep -q 'mktemp -- "\$d/\.\$b\.XXXXXX"' \
    || wt_struct_fail="$wt_struct_fail no-mktemp-template"
  printf '%s\n' "$wt_body" | grep -qF '.new"' && wt_struct_fail="$wt_struct_fail hardcoded-staging-name"
  printf '%s\n' "$wt_body" | grep -q 'mv -fT -- "\$t" "\$p"' \
    || wt_struct_fail="$wt_struct_fail no-mv-T"
  [ "$wt_privcalls" -eq 1 ] || wt_struct_fail="$wt_struct_fail privileged-invocations=$wt_privcalls"
  printf '%s\n' "$wt_body" | grep -q '"\$@" sh -c' || wt_struct_fail="$wt_struct_fail not-a-single-sh-c"
  if [ -z "$wt_struct_fail" ]; then
    ok "perf-capability: STRUCTURAL — the staged install is ONE privileged 'sh -c' (mktemp + write + chmod + mv all inside it, which NARROWS the create-to-reopen window but does NOT close it — see the note above), the staging name comes from an mktemp random-suffix template with no hardcoded literal, and the rename carries -T (#3261 roborev-1/roborev-2)"
  else
    bad "perf-capability: the staged install lost a structural property:$wt_struct_fail"
    printf '%s\n' "$wt_body" | grep -n '\$@\|mktemp\|mv -' | head -8
  fi
  # ...and a `mktemp` that answers OUTSIDE the validated directory is refused rather than trusted —
  # the check now lives INSIDE the privileged shell, so this also proves it survived consolidation.
  wt_mt="$tmp/wt-bad-mktemp"; mkdir -p "$wt_mt"
  for t in bash sh cat printf tee mv rm chmod env grep; do
    s=$(command -v "$t" 2>/dev/null) && ln -sf "$s" "$wt_mt/$t"
  done
  printf '%s\n' '#!/usr/bin/env bash' 'printf "%s\n" "/tmp/perf-cap-elsewhere.$$"' >"$wt_mt/mktemp"
  chmod +x "$wt_mt/mktemp"
  wt_mt_out=$(env PATH="$wt_mt:$PATH" CQLITE_PERF_SYSCTL_DIR="$wt_d" \
    bash -c '. "$1"; perf_capability_dropin_install' _ "$PERFLIB" 2>&1); wt_mt_rc=$?
  if [ "$wt_mt_rc" -ne 0 ] \
     && printf '%s' "$wt_mt_out" | grep -q 'mktemp did not create a staging entry inside the validated directory' \
     && [ ! -e "/tmp/perf-cap-elsewhere.$$" ]; then
    ok "perf-capability: a 'mktemp' answering a path OUTSIDE the validated directory is REFUSED by name from INSIDE the privileged shell, and that path is never created (the tool's answer is checked, not trusted)"
  else
    bad "perf-capability: a mktemp answering outside the validated directory was trusted (rc=$wt_mt_rc, out='$wt_mt_out')"
  fi
  # ...and the POST-CREATION destination race: a symlink-to-DIRECTORY planted at the managed name.
  # Without `mv -T` (--no-target-directory) `mv` would move the staging file INTO the linked
  # directory — the rename that exists to avoid FOLLOWING a symlink would follow one instead, landing
  # the managed bytes outside the sandbox under a different name. With -T the destination is always a
  # plain name to replace. Asserted by consequence: nothing may appear inside the outside directory.
  wt_outdir="$tmp/wt-outside-dir"; rm -rf "$wt_outdir"; mkdir -p "$wt_outdir"
  rm -f "$wt_d/99-cqlite-perf.conf"; ln -s "$wt_outdir" "$wt_d/99-cqlite-perf.conf"
  wt_td=$(env CQLITE_PERF_SYSCTL_DIR="$wt_d" \
    bash -c '. "$1"; perf_capability_dropin_install' _ "$PERFLIB" 2>&1); wt_td_rc=$?
  wt_outdir_contents=$(ls -A "$wt_outdir")
  wt_td_leftover=$(ls -A "$wt_d" | grep -v '^99-cqlite-perf\.conf$' || true)
  # Measured without `-T`: the staging entry landed INSIDE $wt_outdir as `.99-cqlite-perf.conf.XXXXXX`
  # — the managed bytes escaped the sandbox under a name nothing tracks. With `-T`: rc 0, the symlink
  # is REPLACED by a regular file, the outside directory stays empty. All four pinned.
  if [ "$wt_td_rc" -eq 0 ] && [ -z "$wt_outdir_contents" ] && [ -z "$wt_td_leftover" ] \
     && [ -f "$wt_d/99-cqlite-perf.conf" ] && [ ! -L "$wt_d/99-cqlite-perf.conf" ]; then
    ok "perf-capability: a symlink-to-DIRECTORY planted at the managed name does NOT redirect the staged write into it ('mv -T') — the link is REPLACED by a regular file, the outside directory stays empty, no staging entry is left behind (#3261 roborev-2)"
  else
    bad "perf-capability: the staged write was redirected through a symlink-to-directory at the managed name (rc=$wt_td_rc, outside-dir='$wt_outdir_contents', leftover='$wt_td_leftover', out='$wt_td')"
  fi
  rm -f "$wt_d/99-cqlite-perf.conf"
  # ...and THE PRECONDITION THAT ACTUALLY CLOSES THE STAGING RACE (issue #3261, roborev round 3): a
  # (rationale: fleet-runbook.md, perf seam containment, and-the-precondition-that-actually-closes-t)
  wt_perm_d="$tmp/wt-perm.d"
  wt_perm_fail=0
  for wt_mode in 0775 0777 0757; do
    rm -rf "$wt_perm_d"; mkdir -p "$wt_perm_d"; chmod "$wt_mode" "$wt_perm_d"
    wt_perm_out=$(env CQLITE_PERF_SYSCTL_DIR="$wt_perm_d" \
      bash -c '. "$1"; perf_capability_dropin_install' _ "$PERFLIB" 2>&1); wt_perm_rc=$?
    wt_perm_left=$(ls -A "$wt_perm_d")
    if [ "$wt_perm_rc" -eq 0 ] \
       || ! printf '%s' "$wt_perm_out" | grep -q 'group- or world-writable' \
       || [ -n "$wt_perm_left" ]; then
      bad "perf-capability: a mode-$wt_mode drop-in directory was accepted for a privileged staged write (rc=$wt_perm_rc, left='$wt_perm_left', out='$wt_perm_out')"
      wt_perm_fail=1
    fi
  done
  # ...the NEGATIVE CONTROL: a directory owned by the writer and not group/world-writable installs.
  rm -rf "$wt_perm_d"; mkdir -p "$wt_perm_d"; chmod 0755 "$wt_perm_d"
  wt_ok_out=$(env CQLITE_PERF_SYSCTL_DIR="$wt_perm_d" \
    bash -c '. "$1"; perf_capability_dropin_install' _ "$PERFLIB" 2>&1); wt_ok_rc=$?
  if [ "$wt_ok_rc" -ne 0 ] || [ ! -f "$wt_perm_d/99-cqlite-perf.conf" ]; then
    bad "perf-capability: a correctly-owned 0755 drop-in directory was REFUSED — the writability precondition is vacuous (rc=$wt_ok_rc, out='$wt_ok_out')"
    wt_perm_fail=1
  fi
  # ...and an UNDETERMINABLE owner/mode is a refusal, not an assumption: with `stat` unusable the
  # install must fail closed rather than proceed on the hope that the directory is fine.
  wt_nostat="$tmp/wt-nostat"; mkdir -p "$wt_nostat"
  for t in bash sh cat printf tee mv rm chmod env grep mktemp id; do
    s=$(command -v "$t" 2>/dev/null) && ln -sf "$s" "$wt_nostat/$t"
  done
  printf '%s\n' '#!/usr/bin/env bash' 'exit 1' >"$wt_nostat/stat"; chmod +x "$wt_nostat/stat"
  rm -rf "$wt_perm_d"; mkdir -p "$wt_perm_d"; chmod 0755 "$wt_perm_d"
  wt_ns_out=$(env PATH="$wt_nostat" CQLITE_PERF_SYSCTL_DIR="$wt_perm_d" \
    bash -c '. "$1"; perf_capability_dropin_install' _ "$PERFLIB" 2>&1); wt_ns_rc=$?
  # A `stat` that cannot answer AT ALL is now reported as an UNSUPPORTED HOST (rc 2) rather than as
  # "owner/mode indeterminate" — a broken `stat -c` means this host cannot do the atomic install,
  # which is the more accurate diagnosis and the one added in roborev round 16. The property under
  # test is unchanged and is what matters: it FAILS CLOSED and NAMES why, never proceeding on a
  # directory whose ownership was never established. Either wording satisfies that; silence does not.
  if [ "$wt_ns_rc" -eq 0 ] \
     || ! printf '%s' "$wt_ns_out" | grep -qE 'cannot determine owner/mode|UNSUPPORTED on this host'; then
    bad "perf-capability: an unusable 'stat' did not fail closed with a named reason (rc=$wt_ns_rc, out='$wt_ns_out')"
    wt_perm_fail=1
  fi
  # ...a SHORT MODE from `stat -c %a` must not bypass the write-bit check (roborev round 5, High).
  # (rationale: fleet-runbook.md, perf seam containment, a-short-mode-from-stat-c-a-must-not-bypass)
  wt_short="$tmp/wt-shortmode"; mkdir -p "$wt_short"
  for st in bash sh cat printf tee mv rm chmod env grep mktemp id ls; do
    s=$(command -v "$st" 2>/dev/null) && ln -sf "$s" "$wt_short/$st"
  done
  printf '%s\n' '#!/usr/bin/env bash' 'printf "%s 33\n" "$(id -u)"' >"$wt_short/stat"
  chmod +x "$wt_short/stat"
  rm -rf "$wt_perm_d"; mkdir -p "$wt_perm_d"; chmod 0755 "$wt_perm_d"
  wt_sm_out=$(env PATH="$wt_short" CQLITE_PERF_SYSCTL_DIR="$wt_perm_d" \
    bash -c '. "$1"; perf_capability_dropin_install' _ "$PERFLIB" 2>&1); wt_sm_rc=$?
  if [ "$wt_sm_rc" -eq 0 ] || [ -e "$wt_perm_d/99-cqlite-perf.conf" ] \
     || ! printf '%s' "$wt_sm_out" | grep -q 'group- or world-writable'; then
    bad "perf-capability: a SHORT mode (stat reported '33' = 0033, group+world writable) was accepted — the zero-padding is missing or ineffective (rc=$wt_sm_rc, out='$wt_sm_out')"
    wt_perm_fail=1
  fi

  # ...a SYMLINKED destination directory is refused OUTRIGHT (owner ruling A', condition 2: lstat
  # semantics ASSERTED, not inherited from `stat`'s default). The link itself may look perfectly
  # owned and 0755 while entries would be created somewhere else entirely, so measuring the link
  # and proceeding is exactly the by-name reasoning this family has punished eleven times. The
  # link target here is a legitimate, correctly-owned, non-group-writable directory precisely so
  # the case cannot pass for the wrong reason: only the SYMLINK-NESS may cause the refusal.
  wt_ln_target="$tmp/wt-ln-target"; wt_ln_dir="$tmp/wt-ln-dir"
  rm -rf "$wt_ln_target" "$wt_ln_dir"; mkdir -p "$wt_ln_target"; chmod 0755 "$wt_ln_target"
  ln -s "$wt_ln_target" "$wt_ln_dir"
  # ...and the refusal must survive TRAILING SLASHES (roborev round 10, Low). `[ -L link/ ]` and
  # `[ -L link// ]` are both FALSE even when `link` IS a symlink, because the test follows the slash,
  # so one extra character used to walk past the refusal this function explicitly promises. All three
  # spellings are asserted, and the link TARGET is checked unwritten after each one.
  for wt_ln_spell in "$wt_ln_dir" "$wt_ln_dir/" "$wt_ln_dir//"; do
    wt_ln_out=$(env CQLITE_PERF_SYSCTL_DIR="$wt_ln_spell" \
      bash -c '. "$1"; perf_capability_dropin_install' _ "$PERFLIB" 2>&1); wt_ln_rc=$?
    if [ "$wt_ln_rc" -eq 0 ] || ! printf '%s' "$wt_ln_out" | grep -q 'is a SYMLINK'; then
      bad "perf-capability: a SYMLINKED drop-in directory spelled '$wt_ln_spell' was not refused (rc=$wt_ln_rc, out='$wt_ln_out')"
      wt_perm_fail=1
    fi
    if [ -e "$wt_ln_target/99-cqlite-perf.conf" ]; then
      bad "perf-capability: spelling '$wt_ln_spell' was refused but the link TARGET was written anyway"
      wt_perm_fail=1
    fi
  done
  [ "$wt_perm_fail" -ne 0 ] || ok "perf-capability: a privileged staged install REFUSES a group-/world-writable drop-in directory by name and writes nothing, refuses a SYMLINKED destination outright (lstat semantics asserted, target left unwritten), refuses when owner/mode cannot be determined, and still installs into a correctly-owned 0755 directory — the staging race is closed at its PRECONDITION rather than by trying to win it (#3261 roborev-3, owner A' condition 2)"
else
  skip "perf-capability: staged-install write-target cases (symlinked managed name refused; dropin_current does not follow it; the write REPLACES the directory entry)" "no GNU stat -c / mv --no-target-directory on this host"
  skip "perf-capability: staged-install staging-race cases (unpredictable mktemp name; ONE privileged sh -c; mktemp answer outside the validated dir refused; symlink-to-directory not followed)" "no GNU stat -c / mv --no-target-directory on this host"
  skip "perf-capability: staged-install writability-precondition cases (group/world-writable refused; SHORT mode 0033 refused; SYMLINKED destination refused; undeterminable owner/mode fails closed; correctly-owned 0755 still installs)" "no GNU stat -c / mv --no-target-directory on this host"
fi

# ...and CR/LF IN A PATH SEAM IS REFUSED (issue #3261, roborev round 3). Not a containment defect —
# the path IS contained — a SERIALIZATION one, which is why nine rounds of containment work never
# saw it: the search path is emitted ONE ENTRY PER LINE and read back line-wise, so a contained
# directory NAMED with an embedded newline splits into TWO entries, the second being the host's real
# /etc/sysctl.d. One contained path becomes two paths, one of them production.
wt_nl_root="$tmp/wt-nl-root"; mkdir -p "$wt_nl_root"; : >"$wt_nl_root/.cqlite-perf-sandbox"
wt_nl_seam="$wt_nl_root/evil
/etc/sysctl.d"
mkdir -p "$wt_nl_seam" 2>/dev/null
wt_nl_out=$(env CQLITE_PERF_TEST_SANDBOX="$wt_nl_root" CQLITE_PERF_SYSCTL_DIR="$wt_nl_seam" \
  bash -c '. "$1"; perf_capability_sysctl_search_path' _ "$PERFLIB" 2>/dev/null); wt_nl_rc=$?
# The primary seam must be VALID and INSIDE this sandbox (roborev round 28, Low): it previously pointed at
# $seamed_d, which lies OUTSIDE $wt_nl_root, so the PRIMARY seam was refused and the run never reached the
# newline-bearing EXTRA entry this case exists to judge — it passed vacuously for the wrong reason.
wt_nl_primary="$wt_nl_root/primary-sysctl.d"; mkdir -p "$wt_nl_primary"; chmod 0755 "$wt_nl_primary"
# ...proven non-vacuous first: with the newline entry ABSENT the same primary must SUCCEED, otherwise the
# refusal below could still be coming from the primary rather than from the extra entry.
wt_nl_base_rc=0
env CQLITE_PERF_TEST_SANDBOX="$wt_nl_root" CQLITE_PERF_SYSCTL_DIR="$wt_nl_primary" \
  bash -c '. "$1"; perf_capability_sysctl_search_path' _ "$PERFLIB" >/dev/null 2>&1 || wt_nl_base_rc=$?
wt_nl_extra=$(env CQLITE_PERF_TEST_SANDBOX="$wt_nl_root" CQLITE_PERF_SYSCTL_DIR="$wt_nl_primary" \
  CQLITE_PERF_SYSCTL_EXTRA_DIRS="$wt_nl_seam" \
  bash -c '. "$1"; perf_capability_sysctl_search_path' _ "$PERFLIB" 2>/dev/null); wt_nl_extra_rc=$?
if [ "$wt_nl_rc" -ne 0 ] && [ "$wt_nl_extra_rc" -ne 0 ] && [ "$wt_nl_base_rc" -eq 0 ] \
   && ! printf '%s\n' "$wt_nl_out" | grep -qx -- /etc/sysctl.d \
   && ! printf '%s\n' "$wt_nl_extra" | grep -qx -- /etc/sysctl.d; then
  ok "perf-capability: a CONTAINED path carrying an embedded newline is REFUSED as a seam and as an extra-dirs entry, so it can never SERIALIZE into two search-path entries whose second line is the host's real /etc/sysctl.d (#3261 roborev-3)"
else
  bad "perf-capability: embedded-newline handling wrong (seam rc=$wt_nl_rc '$wt_nl_out'; extra rc=$wt_nl_extra_rc '$wt_nl_extra'; BASELINE rc=$wt_nl_base_rc must be 0 or the extra case proves nothing)"
fi
# ...and a CR is refused for the same reason (a CRLF-authored value would leave a stray \r inside a
# resolved entry), while the predicate stays non-vacuous on an ordinary path.
if env bash -c '. "$1"; perf_capability_path_lines_ok "$2"' _ "$PERFLIB" "$(printf '/tmp/a\rb')"; then
  bad "perf-capability: a path containing CR was accepted by the line-safety predicate"
elif ! env bash -c '. "$1"; perf_capability_path_lines_ok "$2"' _ "$PERFLIB" /tmp/ordinary/path; then
  bad "perf-capability: the line-safety predicate rejects an ordinary path (vacuous)"
else
  ok "perf-capability: the line-safety predicate rejects CR as well as LF and still accepts an ordinary path"
fi

# ...and an UNSUPPORTED HOST is reported as rc 2, distinct from rc 1 REFUSED (roborev rounds 16-17).
# The staged install needs GNU `stat -c` and `mv -T`; bootstrap gates the perf section on
# PLATFORM=linux, which is NOT the same as GNU, so a musl/busybox Linux host used to die on a raw tool
# error. The tools are exercised INSIDE the privileged shell (sudo applies its own secure_path, so a
# caller-side probe can check a different binary than the one that will run) and `mv -T` is EXERCISED
# rather than grepped out of --help. The all-GNU control is what stops this passing by refusing always.
us_fail=0
# GUARDED like the staged-install cases (roborev round 21, Medium): this group's CONTROL expects a
# successful install using the HOST's stat/mv, so off GNU it necessarily returns rc 2 and the suite
# fails — reintroducing the macOS breakage the counted skip fixed.
if ! perf_install_supported; then
  skip "perf-capability: UNSUPPORTED-host reporting cases (rc 2 for broken stat -c / mv -T, with an all-GNU control)" "no GNU stat -c / mv --no-target-directory on this host, so the control case cannot install"
else
for us_break in '' mv stat; do
  us_root=$(mktemp -d "$tmp/us.XXXXXX"); : >"$us_root/.cqlite-perf-sandbox"
  mkdir -p "$us_root/sysctl.d"; chmod 0755 "$us_root" "$us_root/sysctl.d"
  us_shim=$(mktemp -d "$tmp/usbin.XXXXXX")
  for us_t in bash sh cat printf tee rm chmod env grep mktemp id ls stat mv; do
    [ "$us_t" = "$us_break" ] && continue
    us_p=$(command -v "$us_t" 2>/dev/null) && ln -sf "$us_p" "$us_shim/$us_t"
  done
  [ -n "$us_break" ] && { printf '%s\n' '#!/bin/sh' 'exit 1' >"$us_shim/$us_break"; chmod +x "$us_shim/$us_break"; }
  us_out=$(env PATH="$us_shim" CQLITE_PERF_TEST_MODE=1 CQLITE_PERF_TEST_SANDBOX="$us_root" \
    CQLITE_PERF_SYSCTL_DIR="$us_root/sysctl.d" CQLITE_PERF_PROC_DIR="$us_root" \
    CQLITE_PERF_TEST_PRIV_DIR="$us_shim" \
    bash -c '. "$1"; perf_capability_dropin_install' _ "$PERFLIB" 2>&1); us_rc=$?
  us_wrote=no; [ -s "$us_root/sysctl.d/99-cqlite-perf.conf" ] && us_wrote=yes
  if [ -z "$us_break" ]; then
    [ "$us_rc" -eq 0 ] && [ "$us_wrote" = yes ] \
      || { bad "perf-capability: an all-GNU host did not install (rc=$us_rc wrote=$us_wrote) — the UNSUPPORTED probe refuses everything"; us_fail=1; }
  else
    [ "$us_rc" -eq 2 ] \
      || { bad "perf-capability: a broken '$us_break' gave rc=$us_rc, not the distinct rc 2 UNSUPPORTED (out='$us_out')"; us_fail=1; }
    printf '%s' "$us_out" | grep -q 'UNSUPPORTED on this host' \
      || { bad "perf-capability: a broken '$us_break' failed without naming the host as unsupported (out='$us_out')"; us_fail=1; }
    [ "$us_wrote" = no ] \
      || { bad "perf-capability: a broken '$us_break' wrote the drop-in anyway"; us_fail=1; }
  fi
done
[ "$us_fail" -ne 0 ] || ok "perf-capability: a non-GNU host is reported as rc 2 UNSUPPORTED by name and writes nothing (broken stat -c and broken mv -T both), while an all-GNU host still installs (#3261 roborev-16/17)"
fi

# ...and an EXTRA_DIRS value whose FIRST LINE is VALID must still be refused (roborev round 31, Medium).
# `read` consumes only the first line, so a value like "<contained-dir>\n/etc/sysctl.d" previously SUCCEEDED
# while silently discarding the remainder -- the scan then reported on an incomplete set, which is the
# falsely-reassuring answer the diagnostic exists to prevent. Round 3 validated the SPLIT ENTRIES and never
# the value being split, so a newline HID entries rather than forging one. The baseline runs first, without
# the newline, and must SUCCEED -- otherwise the refusal proves nothing.
ed_root=$(mktemp -d "$tmp/ed.XXXXXX"); : >"$ed_root/.cqlite-perf-sandbox"
mkdir -p "$ed_root/good" "$ed_root/primary"; chmod 0755 "$ed_root" "$ed_root/good" "$ed_root/primary"
ed_fail=0
ed_base_rc=0
env CQLITE_PERF_TEST_MODE=1 CQLITE_PERF_TEST_SANDBOX="$ed_root" CQLITE_PERF_SYSCTL_DIR="$ed_root/primary" \
  CQLITE_PERF_SYSCTL_EXTRA_DIRS="$ed_root/good" \
  bash -c '. "$1"; perf_capability_sysctl_search_path' _ "$PERFLIB" >/dev/null 2>&1 || ed_base_rc=$?
[ "$ed_base_rc" -eq 0 ] \
  || { bad "perf-capability: the single-line EXTRA_DIRS BASELINE failed (rc=$ed_base_rc) — the newline refusal below would prove nothing"; ed_fail=1; }
ed_hidden="$ed_root/good"$'\n'"/etc/sysctl.d"
ed_out=$(env CQLITE_PERF_TEST_MODE=1 CQLITE_PERF_TEST_SANDBOX="$ed_root" CQLITE_PERF_SYSCTL_DIR="$ed_root/primary" \
  CQLITE_PERF_SYSCTL_EXTRA_DIRS="$ed_hidden" \
  bash -c '. "$1"; perf_capability_sysctl_search_path' _ "$PERFLIB" 2>&1); ed_rc=$?
[ "$ed_rc" -ne 0 ] \
  || { bad "perf-capability: an EXTRA_DIRS value hiding entries after a newline was ACCEPTED (rc=$ed_rc, out='$ed_out')"; ed_fail=1; }
printf '%s\n' "$ed_out" | grep -qx -- /etc/sysctl.d \
  && { bad "perf-capability: the host /etc/sysctl.d reached the search path through a newline-hidden EXTRA_DIRS entry"; ed_fail=1; }
[ "$ed_fail" -ne 0 ] || ok "perf-capability: an EXTRA_DIRS value whose FIRST line is a valid contained directory is still REFUSED when a newline hides more entries after it, and the host /etc/sysctl.d never reaches the search path — while the same value without the newline succeeds (#3261 roborev-31)"

# ...and a stamped sandbox root ending in '//' must behave IDENTICALLY to one with no trailing slash
# (roborev round 31, Low). Only ONE trailing slash was stripped, so '<root>//' became '<root>/', passed the
# '//' rejection, and then the fork-free containment pattern appended its own separator and refused EVERY
# child -- while the RESOLVING write path still accepted the same root. Read and write disagreeing about
# the same sandbox is worse than either answer alone, so all three spellings are asserted to agree.
ts_root=$(mktemp -d "$tmp/ts.XXXXXX"); : >"$ts_root/.cqlite-perf-sandbox"
mkdir -p "$ts_root/child"; chmod 0755 "$ts_root" "$ts_root/child"
ts_fail=0
for ts_spell in "$ts_root" "$ts_root/" "$ts_root//"; do
  env CQLITE_PERF_TEST_MODE=1 CQLITE_PERF_TEST_SANDBOX="$ts_spell" \
    bash -c '. "$1"; perf_capability_sandbox_ok "$2"' _ "$PERFLIB" "$ts_root/child" 2>/dev/null \
    || { bad "perf-capability: a contained child was REFUSED when the sandbox root was spelled '$ts_spell' (fork-free path disagrees with the resolving path)"; ts_fail=1; }
done
[ "$ts_fail" -ne 0 ] || ok "perf-capability: a stamped sandbox root spelled with no trailing slash, one, or two is normalised identically, so the fork-free and resolving paths cannot disagree about the same sandbox (#3261 roborev-31)"

# ...and a SYMLINKED CONTROL FILE inside a contained PROC_DIR must not be read (roborev round 25,
# Medium). The directory gate proved the DIRECTORY contained and symlink-free and said nothing about its
# ENTRIES, so `perf_event_paranoid` could be a link to the host file and the token would report a real or
# attacker-chosen capability as if it came from the fixture. Same directory-is-not-its-entries lesson as
# AC1, on the read path. The CONTROL is the identical tree with a REAL file, so the refusal cannot be
# passing for an unrelated reason.
pc_root=$(mktemp -d "$tmp/pc.XXXXXX"); : >"$pc_root/.cqlite-perf-sandbox"
mkdir -p "$pc_root/proc"; chmod 0755 "$pc_root" "$pc_root/proc"
printf '3\n' >"$pc_root/outside-paranoid"; printf '0\n' >"$pc_root/proc/kptr_restrict"
ln -s "$pc_root/outside-paranoid" "$pc_root/proc/perf_event_paranoid"
pc_link=$(env CQLITE_PERF_TEST_MODE=1 CQLITE_PERF_TEST_SANDBOX="$pc_root" CQLITE_PERF_PROC_DIR="$pc_root/proc" \
  bash -c '. "$1"; perf_capability_token' _ "$PERFLIB" 2>/dev/null)
rm -f "$pc_root/proc/perf_event_paranoid"; printf '3\n' >"$pc_root/proc/perf_event_paranoid"
pc_real=$(env CQLITE_PERF_TEST_MODE=1 CQLITE_PERF_TEST_SANDBOX="$pc_root" CQLITE_PERF_PROC_DIR="$pc_root/proc" \
  bash -c '. "$1"; perf_capability_token' _ "$PERFLIB" 2>/dev/null)
if [ "$pc_link" = absent ] && [ "$pc_real" = paranoid-3 ]; then
  ok "perf-capability: a SYMLINKED control file inside a contained PROC_DIR is refused (token 'absent', never a fabricated capability) while the same tree with a REAL file reads normally (#3261 roborev-25)"
else
  bad "perf-capability: symlinked control file handling wrong (symlinked token='$pc_link' expected absent; real token='$pc_real' expected paranoid-3)"
fi

# ...and LINE-SAFETY MUST BE JUDGED ON THE ORIGINAL PATH, not the canonicalized one (roborev round
# 12, Medium). `$(cd -P -- "$p" && pwd -P)` STRIPS trailing newlines, so a directory whose name ends
# in LF used to pass: the check only ever saw the stripped form, while every later caller emitted the
# ORIGINAL spelling and split the one-per-line search path in two. Round 3 added the CR/LF guard for
# exactly that split; it was running too late to see it. Both variants are pinned — a directory whose
# name ends in LF, and a file whose PARENT ends in LF — because they canonicalize by different routes.
lf_root=$(mktemp -d "$tmp/lf.XXXXXX"); : >"$lf_root/.cqlite-perf-sandbox"
lf_dir="$lf_root/evil"$'\n'
lf_fail=0
if mkdir -p "$lf_dir" 2>/dev/null; then
  if env CQLITE_PERF_TEST_MODE=1 CQLITE_PERF_TEST_SANDBOX="$lf_root" \
       bash -c '. "$1"; perf_capability_sandbox_ok_resolved "$2"' _ "$PERFLIB" "$lf_dir" 2>/dev/null; then
    bad "perf-capability: a directory whose name ends in LF was ACCEPTED by the resolved containment check (the newline was laundered through pwd -P)"
    lf_fail=1
  fi
  # ...and the FILE variant, whose parent is the LF-named directory.
  : >"$lf_dir/sysctl.conf" 2>/dev/null
  if env CQLITE_PERF_TEST_MODE=1 CQLITE_PERF_TEST_SANDBOX="$lf_root" \
       bash -c '. "$1"; perf_capability_sandbox_file_ok_resolved "$2"' _ "$PERFLIB" "$lf_dir/sysctl.conf" 2>/dev/null; then
    bad "perf-capability: a file whose PARENT directory name ends in LF was ACCEPTED by the resolved file containment check"
    lf_fail=1
  fi
  # ...NEGATIVE CONTROL: the same shapes WITHOUT a newline are still accepted, so the check is not
  # refusing every path that merely looks unusual.
  mkdir -p "$lf_root/ordinary"
  env CQLITE_PERF_TEST_MODE=1 CQLITE_PERF_TEST_SANDBOX="$lf_root" \
    bash -c '. "$1"; perf_capability_sandbox_ok_resolved "$2"' _ "$PERFLIB" "$lf_root/ordinary" 2>/dev/null \
    || { bad "perf-capability: an ORDINARY contained directory was refused by the line-safety ordering fix (vacuous)"; lf_fail=1; }
  [ "$lf_fail" -ne 0 ] || ok "perf-capability: line-safety is judged on the ORIGINAL path — a directory ending in LF and a file whose parent ends in LF are both REFUSED (the newline cannot launder through pwd -P), while an ordinary contained directory still passes (#3261 roborev-12)"
else
  skip "perf-capability: LF-in-path containment cases" "this filesystem refused to create a directory whose name contains a newline"
fi

# ...and the COMPETING-FILE SCAN must validate every globbed file, not just its directory (roborev
# round 11, Medium). `[ -f ]` and `grep` FOLLOW symlinks, so a link sitting inside a perfectly
# contained sandbox directory but pointing at a real host `*.conf` was read, and its contents
# fabricated "a competitor sets these keys" diagnostics out of HOST state — the asserted numbers
# would come from the box instead of the fixture. Fails CLOSED: a competitor we declined to examine
# is the UNKNOWN this diagnostic exists to report, not hide.
cs_root=$(mktemp -d "$tmp/cs.XXXXXX"); : >"$cs_root/.cqlite-perf-sandbox"
cs_dir="$cs_root/sysctl.d"; mkdir -p "$cs_dir"
cs_host="$tmp/cs-host-competitor.conf"
printf 'kernel.perf_event_paranoid = 2\n' >"$cs_host"
ln -s "$cs_host" "$cs_dir/00-host-link.conf"
cs_fail=0
cs_out=$(env CQLITE_PERF_TEST_MODE=1 CQLITE_PERF_TEST_SANDBOX="$cs_root" \
  CQLITE_PERF_SYSCTL_DIR="$cs_dir" CQLITE_PERF_PROC_DIR="$cs_root" \
  bash -c '. "$1"; perf_capability_competing_files' _ "$PERFLIB" 2>&1); cs_rc=$?
[ "$cs_rc" -ne 0 ] || { bad "perf-capability: the competing-file scan ACCEPTED a symlinked *.conf inside the sandbox (rc=$cs_rc, out='$cs_out')"; cs_fail=1; }
printf '%s' "$cs_out" | grep -q 'REFUSING to scan' \
  || { bad "perf-capability: the scan failed on a symlinked competitor without naming the refusal (out='$cs_out')"; cs_fail=1; }
printf '%s' "$cs_out" | grep -q 'perf_event_paranoid = 2' \
  && { bad "perf-capability: the scan LEAKED host competitor content through a symlink"; cs_fail=1; }
# ...the NEGATIVE CONTROL: a REAL file inside the sandbox is still scanned, so the check is not
# refusing everything.
printf 'kernel.kptr_restrict = 1\n' >"$cs_dir/zz-real.conf" 2>/dev/null
# ...and the NEWLINE-BASENAME case, now actually EXERCISED (roborev round 27, Low). This block used to
# (full rationale: fleet-runbook.md, perf seam containment, and-the-newline-basename-case-now-actually-ex)
cs_nl_dir="$cs_root/nl-sysctl.d"; rm -rf "$cs_nl_dir"; mkdir -p "$cs_nl_dir"; chmod 0755 "$cs_nl_dir"
cs_nl_name=$(printf 'zz-nl\ncompetitor.conf')
# BASELINE FIRST, WITHOUT the newline file, and its status REQUIRED (roborev round 30, Low). My previous
# version ran the "ordinary" baseline while the newline-named file was ALREADY present, making it identical
# to the refusal case, and then discarded its status with `|| true` — so the negative control controlled
# nothing. Three iterations of this one case have now been vacuous in a different way each time; the
# pattern in my own work is that I fix the assertion and forget to re-check that it can still reach its
# subject. Ordinary file only -> MUST scan (rc 0). Then add the newline file -> MUST fail closed.
printf 'kernel.kptr_restrict = 1\n' >"$cs_nl_dir/zz-ordinary.conf"
cs_nl_base=$(env CQLITE_PERF_TEST_MODE=1 CQLITE_PERF_TEST_SANDBOX="$cs_root" \
  CQLITE_PERF_SYSCTL_DIR="$cs_nl_dir" CQLITE_PERF_PROC_DIR="$cs_root" \
  bash -c '. "$1"; perf_capability_competing_files' _ "$PERFLIB" 2>&1); cs_nl_base_rc=$?
[ "$cs_nl_base_rc" -eq 0 ] \
  || { bad "perf-capability: the ordinary-competitor BASELINE failed (rc=$cs_nl_base_rc, out='$cs_nl_base') — the newline refusal below would prove nothing"; cs_fail=1; }
if printf 'kernel.kptr_restrict = 1\n' >"$cs_nl_dir/$cs_nl_name" 2>/dev/null; then
  cs_nl_out=$(env CQLITE_PERF_TEST_MODE=1 CQLITE_PERF_TEST_SANDBOX="$cs_root" \
    CQLITE_PERF_SYSCTL_DIR="$cs_nl_dir" CQLITE_PERF_PROC_DIR="$cs_root" \
    bash -c '. "$1"; perf_capability_competing_files' _ "$PERFLIB" 2>&1); cs_nl_rc=$?
  [ "$cs_nl_rc" -ne 0 ] \
    || { bad "perf-capability: a newline-named competitor did NOT fail the scan closed (rc=$cs_nl_rc, out='$cs_nl_out')"; cs_fail=1; }
  printf '%s' "$cs_nl_out" | grep -qx -- 'competitor.conf' \
    && { bad "perf-capability: a newline in a competitor BASENAME split into an extra reported entry (out='$cs_nl_out')"; cs_fail=1; }
  rm -f -- "$cs_nl_dir/$cs_nl_name"
else
  skip "perf-capability: newline-in-basename competitor case" "this filesystem refused to create a filename containing a newline"
fi
cs_ok_out=$(env CQLITE_PERF_TEST_MODE=1 CQLITE_PERF_TEST_SANDBOX="$cs_root" \
  CQLITE_PERF_SYSCTL_DIR="$cs_dir" CQLITE_PERF_PROC_DIR="$cs_root" \
  bash -c '. "$1"; perf_capability_competing_files' _ "$PERFLIB" 2>&1); cs_ok_rc=$?
rm -f -- "$cs_dir/00-host-link.conf"
cs_ok2_out=$(env CQLITE_PERF_TEST_MODE=1 CQLITE_PERF_TEST_SANDBOX="$cs_root" \
  CQLITE_PERF_SYSCTL_DIR="$cs_dir" CQLITE_PERF_PROC_DIR="$cs_root" \
  bash -c '. "$1"; perf_capability_competing_files' _ "$PERFLIB" 2>&1); cs_ok2_rc=$?
if [ "$cs_ok2_rc" -ne 0 ] || ! printf '%s' "$cs_ok2_out" | grep -q 'zz-real.conf'; then
  bad "perf-capability: a REAL contained competitor was not scanned once the symlink was removed — the per-file check is refusing everything (rc=$cs_ok2_rc, out='$cs_ok2_out')"
  cs_fail=1
fi
[ "$cs_fail" -ne 0 ] || ok "perf-capability: the competing-file scan validates EVERY globbed file in test mode — a symlinked *.conf inside the sandbox fails the scan CLOSED by name and leaks no host content, while a real contained competitor is still reported (#3261 roborev-11)"

# ...and a FAILING CONTENT GENERATOR must never look like success (roborev round 9, Medium). Both
# call sites used to lose the generator's status: dropin_current ran a trailing sentinel `printf`
# whose rc replaced it, so against an EMPTY file the compare was "X" == "X" and reported the drop-in
# ALREADY CURRENT; dropin_install piped the generator into the privileged shell, so the pipeline's rc
# was the last command's and a failure only surfaced if the CALLER had `pipefail`. Both are vacuous
# positives from an unmeasured state. No GNU-only tooling is exercised here: each must fail BEFORE
# any privileged command runs, which is the property under test.
cg_dir=$(mktemp -d "$tmp/cg.XXXXXX"); : >"$cg_dir/99-cqlite-perf.conf"
cg_fail=0
cg_cur_rc=0
env CQLITE_PERF_SYSCTL_DIR="$cg_dir" bash -c '
  . "$1"
  perf_capability_dropin_content() { return 1; }
  perf_capability_dropin_current' _ "$PERFLIB" >/dev/null 2>&1 || cg_cur_rc=$?
[ "$cg_cur_rc" -ne 0 ] || { bad "perf-capability: a FAILING content generator let dropin_current report the drop-in already current (empty file compared equal)"; cg_fail=1; }
cg_ins_out=$(env CQLITE_PERF_SYSCTL_DIR="$cg_dir" bash -c '
  . "$1"
  perf_capability_dropin_content() { return 1; }
  perf_capability_dropin_install' _ "$PERFLIB" 2>&1); cg_ins_rc=$?
[ "$cg_ins_rc" -ne 0 ] || { bad "perf-capability: dropin_install succeeded with a FAILING content generator (rc=$cg_ins_rc, out='$cg_ins_out')"; cg_fail=1; }
[ ! -s "$cg_dir/99-cqlite-perf.conf" ] || { bad "perf-capability: dropin_install wrote content despite a failing generator"; cg_fail=1; }
[ "$cg_fail" -ne 0 ] || ok "perf-capability: a FAILING drop-in content generator propagates — dropin_current does NOT report 'already current' against an empty file, and dropin_install refuses before any privileged command and writes nothing (#3261 roborev-9)"

# ...and the install is still gated: an out-of-sandbox seam may not be written at all. Same GNU-only
# toolchain dependency as the staged-install block above, so same counted skip off GNU.
if ! perf_install_supported; then
  skip "perf-capability: dropin_install refuses a seam resolving OUT of the sandbox" "no GNU stat -c / mv --no-target-directory on this host"
elif env CQLITE_PERF_SYSCTL_DIR="$symanc/sysctl.d" \
     bash -c '. "$1"; perf_capability_dropin_install' _ "$PERFLIB" >/dev/null 2>&1; then
  bad "perf-capability: dropin_install wrote through a seam resolving OUT of the sandbox"
else
  ok "perf-capability: dropin_install inherits the containment gate — a seam resolving out of the sandbox writes nothing"
fi

# 1g-ii. AC2 (Medium) — the inversion REGRESSED symlink rejection on the READ path. The
# (rationale: fleet-runbook.md, perf seam containment, 1g-ii-ac2-medium-the-inversion-regressed-sym)
ac2_fail=0
ac2_link="$tmp/ac2-proc-is-a-symlink"; rm -f "$ac2_link"; ln -s /proc/sys/kernel "$ac2_link"
ac2_dirlink="$tmp/ac2-ancestor-link"; rm -f "$ac2_dirlink"; ln -s /proc/sys "$ac2_dirlink"
for ac2_seam in "$ac2_link" "$ac2_dirlink/kernel"; do
  ac2_tok=$(env CQLITE_PERF_PROC_DIR="$ac2_seam" bash "$PERFLIB" --token 2>/dev/null)
  [ "$ac2_tok" = absent ] || {
    bad "perf-capability: the fork-free READ path followed a SYMLINK inside the sandbox to the real /proc and reported a FABRICATED token '$ac2_tok' for seam '$ac2_seam' (expected 'absent')"
    ac2_fail=1; }
  env CQLITE_PERF_PROC_DIR="$ac2_seam" \
    bash -c '. "$1"; perf_capability_proc_dir_into d' _ "$PERFLIB" >/dev/null 2>&1 && {
    bad "perf-capability: proc_dir_into ACCEPTED a symlinked seam '$ac2_seam'"; ac2_fail=1; }
done
[ "$ac2_fail" -ne 0 ] || ok "perf-capability: the fork-free READ path rejects a SYMLINKED path component even when the SPELLING is strictly inside the sandbox — a symlink to the real /proc/sys/kernel reads nothing (token 'absent'), never a fabricated capability (#3261 AC2)"
# ...and the rejection is not vacuous: a REAL directory of the same shape still reads.
ac2_real="$tmp/ac2-real-proc"; mkdir -p "$ac2_real"
printf '%s\n' -1 >"$ac2_real/perf_event_paranoid"; printf '%s\n' 0 >"$ac2_real/kptr_restrict"
if [ "$(env CQLITE_PERF_PROC_DIR="$ac2_real" bash "$PERFLIB" --token 2>/dev/null)" = ok ]; then
  ok "perf-capability: a REAL (symlink-free) stand-in directory inside the sandbox still reads — the AC2 rejection is per-component, not a blanket refusal"
else
  bad "perf-capability: the symlink rejection made the fork-free read path vacuous (a real stand-in dir no longer reads)"
fi

# 1g-iii. AC3 (Low) — a STRICTLY CONTAINED file was wrongly REFUSED. The file variant judged
#         its PARENT with the strict-containment predicate, so `<root>/sysctl.conf` failed:
#         the parent IS the root, and a root is not strictly inside itself. The judged path
#         must be <canonical parent>/<basename>, which IS strictly inside. A guard that
#         refuses legitimate input is the guard people learn to work around, so this is a
#         correctness case, not a convenience.
ac3_ok() { env CQLITE_PERF_TEST_SANDBOX="$1" \
  bash -c '. "$1"; perf_capability_sandbox_file_ok_resolved "$2"' _ "$PERFLIB" "$2"; }
: >"$sbx/sysctl.conf"
ac3_fail=0
ac3_ok "$sbx" "$sbx/sysctl.conf" || { bad "perf-capability: '<sandbox-root>/sysctl.conf' was REFUSED though strictly contained"; ac3_fail=1; }
ac3_ok "$sbx" "$sbx/inside/sysctl.conf" || { bad "perf-capability: a sysctl.conf one level deeper inside the sandbox was refused"; ac3_fail=1; }
# ...while every genuine escape is still refused: outside the root, the root itself, a `..`
# spelling, and — the AC1 lesson applied here — a SYMLINKED final component, whose CONTENTS
# the competing-file scan would otherwise read out of the host's real configuration.
rm -f "$sbx/linked-sysctl.conf"; ln -s /etc/sysctl.conf "$sbx/linked-sysctl.conf"
for ac3_bad in "$tmp/outside-the-root/sysctl.conf" "$sbx" "$sbx/../sysctl.conf" \
               "$sbx/linked-sysctl.conf" "relative/sysctl.conf" ''; do
  ac3_ok "$sbx" "$ac3_bad" && { bad "perf-capability: sandbox_file_ok_resolved ACCEPTED '$ac3_bad'"; ac3_fail=1; }
done
[ "$ac3_fail" -ne 0 ] || ok "perf-capability: the FILE variant accepts a strictly-contained '<root>/sysctl.conf' (canonical parent + basename) while still refusing one outside the root, the root itself, a '..' spelling, a relative path and a SYMLINKED final component (#3261 AC3)"
# ...and end-to-end through the seam that consumes it: a contained sysctl.conf stand-in is a
# legitimate CQLITE_PERF_SYSCTL_EXTRA_DIRS entry and must appear on the search path.
: >"$tmp/sysctl.conf"
ac3_path=$(env CQLITE_PERF_SYSCTL_DIR="$seamed_d" CQLITE_PERF_SYSCTL_EXTRA_DIRS="$tmp/sysctl.conf" \
  bash -c '. "$1"; perf_capability_sysctl_search_path' _ "$PERFLIB" 2>/dev/null); ac3_path_rc=$?
if [ "$ac3_path_rc" -eq 0 ] && printf '%s\n' "$ac3_path" | grep -qx -- "$tmp/sysctl.conf"; then
  ok "perf-capability: a sysctl.conf stand-in directly inside the sandbox root is accepted as a CQLITE_PERF_SYSCTL_EXTRA_DIRS entry and reaches the search path (#3261 AC3, end to end)"
else
  bad "perf-capability: a contained sysctl.conf stand-in was dropped from the test-mode search path (rc=$ac3_path_rc, path='$ac3_path')"
fi

# 1g-iv. AC4 (High) — the guard authorizes EXECUTABLES, and never resolved them. Two escapes,
# (rationale: fleet-runbook.md, perf seam containment, 1g-iv-ac4-high-the-guard-authorizes-executab)
ac4_sys=''
if ac4_sys=$(mktemp -d "${TMPDIR:-/tmp}/perf-cap-outside.XXXXXX") && [ -d "$ac4_sys" ]; then
  trap 'rm -rf "$tmp" "$ac4_sys"' EXIT
else
  ac4_sys=''
fi
if [ -z "$ac4_sys" ]; then
  bad "perf-capability: could not create the out-of-sandbox dir the AC4 cases need"
else
  for t in sudo sysctl; do
    printf '#!/usr/bin/env bash\nexit 0\n' >"$ac4_sys/$t"; chmod +x "$ac4_sys/$t"
  done
  ac4_guard() { # ac4_guard <PATH-prefix> <priv-dir> -> rc + stderr; sandbox root is $sbx
    env PATH="$1:$PATH" CQLITE_PERF_TEST_MODE=1 CQLITE_PERF_TEST_SANDBOX="$sbx" \
      CQLITE_PERF_TEST_PRIV_DIR="$2" CQLITE_PERF_PROC_DIR="$sbx/inside-proc" \
      CQLITE_PERF_SYSCTL_DIR="$sbx/inside" \
      bash -c '. "$1"; perf_capability_env_guard' _ "$PERFLIB" 2>&1
  }
  ac4_fail=0
  # (a) the `/usr` shape: absolute, CONTAINS the tools, not in the sandbox.
  ac4_out=$(ac4_guard "$ac4_sys" "$ac4_sys") && ac4_fail=1
  printf '%s' "$ac4_out" | grep -q 'sandbox' || ac4_fail=1
  [ "$ac4_fail" -eq 0 ] || bad "perf-capability: an absolute shim dir OUTSIDE the sandbox root (the '/usr' shape) was accepted, or refused without naming the sandbox: '$ac4_out'"
  # (b) the SYMLINK shape: a declared shim dir genuinely inside the sandbox, whose `sudo`
  #     RESOLVES to the tool outside it.
  ac4_link="$sbx/ac4-shims"; mkdir -p "$ac4_link"
  printf '#!/usr/bin/env bash\nexit 0\n' >"$ac4_link/sysctl"; chmod +x "$ac4_link/sysctl"
  rm -f "$ac4_link/sudo"; ln -s "$ac4_sys/sudo" "$ac4_link/sudo"
  ac4_out2=$(ac4_guard "$ac4_link" "$ac4_link") && {
    bad "perf-capability: a SYMLINK to a real privileged tool inside the declared shim dir was accepted — test mode could run the host's own sudo/sysctl"; ac4_fail=1; }
  # (c) the SWEEP: with NO sudo/sysctl reachable on PATH at all the per-tool loop resolves
  #     nothing, so a symlinked `sysctl` PARKED in the declared shim dir must still be
  #     refused — otherwise it is one PATH-order change away from being executed.
  ac4_nopath="$sbx/ac4-nopath"; mkdir -p "$ac4_nopath"
  for t in bash cat printf env grep; do
    s=$(command -v "$t" 2>/dev/null) && ln -sf "$s" "$ac4_nopath/$t"
  done
  ac4_park="$sbx/ac4-parked"; mkdir -p "$ac4_park"
  rm -f "$ac4_park/sysctl"; ln -s "$ac4_sys/sysctl" "$ac4_park/sysctl"
  ac4_out3=$(env PATH="$ac4_nopath" CQLITE_PERF_TEST_MODE=1 CQLITE_PERF_TEST_SANDBOX="$sbx" \
    CQLITE_PERF_TEST_PRIV_DIR="$ac4_park" CQLITE_PERF_PROC_DIR="$sbx/inside-proc" \
    CQLITE_PERF_SYSCTL_DIR="$sbx/inside" \
    bash -c '. "$1"; perf_capability_env_guard' _ "$PERFLIB" 2>&1) && {
    bad "perf-capability: a privileged tool PARKED as a symlink in the declared shim dir was accepted because PATH did not happen to reach it"; ac4_fail=1; }
  # ...and NOT vacuous: a shim dir inside the sandbox holding REAL FILES is still accepted.
  ac4_good="$sbx/ac4-good-shims"; mkdir -p "$ac4_good"
  for t in sudo sysctl; do
    printf '#!/usr/bin/env bash\nexit 0\n' >"$ac4_good/$t"; chmod +x "$ac4_good/$t"
  done
  ac4_guard "$ac4_good" "$ac4_good" >/dev/null 2>&1 || {
    bad "perf-capability: a legitimate shim dir of REAL FILES inside the sandbox was refused — the AC4 fix is vacuous"; ac4_fail=1; }
  [ "$ac4_fail" -ne 0 ] || ok "perf-capability: every privileged executable the guard authorizes is validated by DESTINATION — a shim dir outside the sandbox ('/usr' shape), a symlink to a real tool inside a declared shim dir, and one merely PARKED there out of PATH reach are all refused, while a shim dir of real files inside the sandbox still works (#3261 AC4)"
fi

# Nothing in this suite may have touched the REAL /etc/sysctl.d.
perf_test_assert_host_clean
perf_test_report
