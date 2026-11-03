#!/usr/bin/env bash
# Self-test for scripts/local/cargo-target-prune.sh (#4411). Hermetic: a fake
# target dir under mktemp, stub pgrep/systemctl, no cargo, no network.
set -uo pipefail

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
SCRIPT="$REPO_ROOT/scripts/local/cargo-target-prune.sh"
PASS=0 FAIL=0
ok()  { echo "PASS - $1"; PASS=$((PASS + 1)); }
bad() { echo "FAIL - $1"; FAIL=$((FAIL + 1)); }
check() { if eval "$2"; then ok "$1"; else bad "$1"; fi; }

tmp=$(mktemp -d); trap 'rm -rf "$tmp"' EXIT
stub="$tmp/stub"; mkdir -p "$stub"
# pgrep: busy iff $tmp/busy-<name> exists. systemctl: log calls; list-units
# prints $tmp/units if present.
cat >"$stub/pgrep" <<EOF
#!/usr/bin/env bash
for a in "\$@"; do n=\$a; done
[ -e "$tmp/busy-\$n" ]
EOF
cat >"$stub/systemctl" <<EOF
#!/usr/bin/env bash
echo "\$*" >>"$tmp/systemctl.log"
case " \$* " in *" list-units "*) [ -f "$tmp/units" ] && cat "$tmp/units" ;; esac
exit 0
EOF
chmod +x "$stub/pgrep" "$stub/systemctl"
export CQLITE_PRUNE_PGREP="$stub/pgrep" CQLITE_PRUNE_SYSTEMCTL="$stub/systemctl"
export CQLITE_PRUNE_PACKAGES="cqlite cqlite-core"

H1=0123456789abcdef H2=fedcba9876543210
mk_target() {
  T="$tmp/target"; rm -rf "$T"
  mkdir -p "$T"
  echo "Signature: 8a477f597d28d172789f06886806bc55" >"$T/CACHEDIR.TAG"
  for prof in debug agent-gate-side/legacy-heuristics/debug; do
    mkdir -p "$T/$prof/.fingerprint" "$T/$prof/incremental"
    : >"$T/$prof/.cargo-lock"
    mkdir -p "$T/$prof/.fingerprint/cqlite-$H1" "$T/$prof/.fingerprint/cqlite-core-$H2" \
             "$T/$prof/.fingerprint/serde-$H1" "$T/$prof/.fingerprint/cqlite-core-notahash"
    mkdir -p "$T/$prof/incremental/old-abc" "$T/$prof/incremental/new-def"
    touch -d '30 days ago' "$T/$prof/incremental/old-abc"
  done
}
run() { out=$(bash "$SCRIPT" --repo "$REPO_ROOT" "$@" 2>&1); rc=$?; }

# 1. dry run deletes nothing but counts what it would remove.
mk_target; run --target "$T" --dry-run
check "dry-run exits 0" '[ "$rc" = 0 ]'
check "dry-run reports DRY-RUN" 'grep -q "^RESULT: DRY-RUN$" <<<"$out"'
check "dry-run counts 2 incremental + 4 fingerprints" 'grep -q "incremental-removed=2 fingerprints-removed=4" <<<"$out"'
check "dry-run removed nothing" '[ -d "$T/debug/incremental/old-abc" ] && [ -d "$T/debug/.fingerprint/cqlite-$H1" ]'

# 2. real prune: old incremental + workspace fingerprints go, everything else stays.
mk_target; run --target "$T"
check "prune exits 0 with PRUNED" '[ "$rc" = 0 ] && grep -q "^RESULT: PRUNED$" <<<"$out"'
for prof in debug agent-gate-side/legacy-heuristics/debug; do
  check "$prof: old incremental removed" '[ ! -e "$T/$prof/incremental/old-abc" ]'
  check "$prof: recent incremental kept" '[ -d "$T/$prof/incremental/new-def" ]'
  check "$prof: workspace fingerprints removed" '[ ! -e "$T/$prof/.fingerprint/cqlite-$H1" ] && [ ! -e "$T/$prof/.fingerprint/cqlite-core-$H2" ]'
  check "$prof: third-party fingerprint kept" '[ -d "$T/$prof/.fingerprint/serde-$H1" ]'
  check "$prof: non-hash suffix kept" '[ -d "$T/$prof/.fingerprint/cqlite-core-notahash" ]'
done

# 3. --days 0 removes the recent incremental dir too (find -mtime +0 = older than 24h,
#    so age it by 2 days first).
mk_target; touch -d '2 days ago' "$T/debug/incremental/new-def"; run --target "$T" --days 0
check "--days 0 removes a 2-day-old incremental dir" '[ ! -e "$T/debug/incremental/new-def" ]'

# 4. busy: a cargo process, an active gate unit, or a held build lock each skip.
for p in cargo rustc cargo-nextest; do
  mk_target; : >"$tmp/busy-$p"; run --target "$T"; rm -f "$tmp/busy-$p"
  check "busy $p: SKIPPED-BUSY, exit 0" '[ "$rc" = 0 ] && grep -q "^RESULT: SKIPPED-BUSY$" <<<"$out" && grep -q "process:$p" <<<"$out"'
  check "busy $p: nothing removed" '[ -d "$T/debug/.fingerprint/cqlite-$H1" ] && [ -d "$T/debug/incremental/old-abc" ]'
done
mk_target; echo "cqlite-gate-20261102T000000Z-1.service loaded active running x" >"$tmp/units"; run --target "$T"; rm -f "$tmp/units"
check "active gate unit: SKIPPED-BUSY naming the unit" 'grep -q "^RESULT: SKIPPED-BUSY$" <<<"$out" && grep -q "unit:cqlite-gate-20261102T000000Z-1.service" <<<"$out"'
check "active gate unit: nothing removed" '[ -d "$T/debug/.fingerprint/cqlite-$H1" ]'
mk_target
flock "$T/agent-gate-side/legacy-heuristics/debug/.cargo-lock" sleep 30 & lockpid=$!
for _ in $(seq 50); do flock -n "$T/agent-gate-side/legacy-heuristics/debug/.cargo-lock" true 2>/dev/null || break; sleep 0.1; done
run --target "$T"; kill "$lockpid" 2>/dev/null; wait "$lockpid" 2>/dev/null
check "held side-lane .cargo-lock: SKIPPED-BUSY naming the lock" 'grep -q "^RESULT: SKIPPED-BUSY$" <<<"$out" && grep -q "lock:agent-gate-side/legacy-heuristics/debug" <<<"$out"'
check "held lock: nothing removed" '[ -d "$T/debug/.fingerprint/cqlite-$H1" ]'

# 5. refusals and usage.
mk_target; rm -f "$T/CACHEDIR.TAG"; run --target "$T"
check "no CACHEDIR.TAG: exit 1, ERROR, nothing removed" '[ "$rc" = 1 ] && grep -q "^RESULT: ERROR$" <<<"$out" && [ -d "$T/debug/.fingerprint/cqlite-$H1" ]'
run --target "$tmp/does-not-exist"
check "missing target: exit 1" '[ "$rc" = 1 ]'
run --days x
check "bad --days: exit 2" '[ "$rc" = 2 ]'
run --bogus
check "unknown flag: exit 2" '[ "$rc" = 2 ]'

# 6. timer install/uninstall into a private config/data home.
export XDG_CONFIG_HOME="$tmp/cfg" XDG_DATA_HOME="$tmp/data"; rm -f "$tmp/systemctl.log"
run --install-timer
check "install: exit 0, INSTALLED" '[ "$rc" = 0 ] && grep -q "^RESULT: INSTALLED$" <<<"$out"'
check "install: copied script is executable" '[ -x "$tmp/data/cqlite/cargo-target-prune.sh" ]'
check "install: service runs the copy with --repo" 'grep -q "ExecStart=/usr/bin/env bash $tmp/data/cqlite/cargo-target-prune.sh --repo $REPO_ROOT" "$tmp/cfg/systemd/user/cqlite-cargo-prune.service"'
check "install: timer is weekly and persistent" 'grep -q "^OnCalendar=Sun" "$tmp/cfg/systemd/user/cqlite-cargo-prune.timer" && grep -q "^Persistent=true" "$tmp/cfg/systemd/user/cqlite-cargo-prune.timer"'
check "install: enabled the timer" 'grep -q "enable --now cqlite-cargo-prune.timer" "$tmp/systemctl.log"'
run --uninstall-timer
check "uninstall: units removed" '[ "$rc" = 0 ] && [ ! -e "$tmp/cfg/systemd/user/cqlite-cargo-prune.timer" ] && [ ! -e "$tmp/cfg/systemd/user/cqlite-cargo-prune.service" ]'

echo "PASS=$PASS FAIL=$FAIL SKIP=0"
[ "$FAIL" = 0 ]
