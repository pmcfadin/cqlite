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
check "held lock: nothing removed (held profile and the free one)" '[ -d "$T/debug/.fingerprint/cqlite-$H1" ] && [ -d "$T/debug/incremental/old-abc" ] && [ -d "$T/agent-gate-side/legacy-heuristics/debug/.fingerprint/cqlite-$H1" ] && [ -d "$T/agent-gate-side/legacy-heuristics/debug/incremental/old-abc" ]'

# 4b. failing busy probes fail closed (SKIPPED-BUSY, named reason, nothing removed).
shim() { printf '#!/usr/bin/env bash\n%s\n' "$2" >"$tmp/$1"; chmod +x "$tmp/$1"; }
probe_case() { want=$4 # label pgrep systemctl reason
 
  mk_target; CQLITE_PRUNE_PGREP=$2 CQLITE_PRUNE_SYSTEMCTL=$3 run --target "$T"
  check "$1: SKIPPED-BUSY naming $want, nothing removed" 'grep -q "^RESULT: SKIPPED-BUSY$" <<<"$out" && grep -qF "$want" <<<"$out" && [ -d "$T/debug/.fingerprint/cqlite-$H1" ] && [ -d "$T/debug/incremental/old-abc" ]'
}
shim pgrep-rc2 'exit 2'
shim systemctl-listfail 'case " $* " in *" list-units "*) exit 1;; esac; exit 0'
probe_case "pgrep exits 2" "$tmp/pgrep-rc2" "$stub/systemctl" "probe-error:pgrep(rc=2)"
probe_case "pgrep missing" "$tmp/no-such-pgrep" "$stub/systemctl" "probe-error:pgrep(rc=127)"
probe_case "systemctl list-units fails" "$stub/pgrep" "$tmp/systemctl-listfail" "probe-error:systemctl-list-units"
# positive control: pgrep exit 1 (no match) plus a succeeding systemctl still prunes.
shim pgrep-rc1 'exit 1'
mk_target; CQLITE_PRUNE_PGREP="$tmp/pgrep-rc1" run --target "$T"
check "pgrep exit 1 (no match) is idle: PRUNED" 'grep -q "^RESULT: PRUNED$" <<<"$out" && [ ! -e "$T/debug/.fingerprint/cqlite-$H1" ]'

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
mk_target
# Absent .cargo-lock: still locked (created) and pruned.
mk_target; rm -f "$T/debug/.cargo-lock"
run --target "$T"
check "absent .cargo-lock: PRUNED, lock file created, fingerprints removed" 'grep -q "^RESULT: PRUNED$" <<<"$out" && [ -f "$T/debug/.cargo-lock" ] && [ ! -e "$T/debug/.fingerprint/cqlite-$H1" ]'
# Absent lock that cannot be created: SKIPPED-BUSY with a named reason, nothing deleted.
mk_target; rm -f "$T/debug/.cargo-lock"; mkdir "$T/debug/.cargo-lock"
run --target "$T"
check "uncreatable .cargo-lock: SKIPPED-BUSY naming it, nothing deleted" 'grep -q "^RESULT: SKIPPED-BUSY$" <<<"$out" && grep -q "lock-uncreatable:debug\|lock:debug" <<<"$out" && [ -d "$T/debug/.fingerprint/cqlite-$H1" ] && [ -d "$T/debug/incremental/old-abc" ]'

# Exact-name match: workspace `foo_bar` removes `foo_bar-<hash>` but never a
# foreign `foo-bar-<hash>` (Cargo keeps the name verbatim on disk).
mk_target; mkdir -p "$T/debug/.fingerprint/foo_bar-$H1" "$T/debug/.fingerprint/foo-bar-$H1"
CQLITE_PRUNE_PACKAGES="foo_bar" run --target "$T"
check "underscore pkg: foo_bar-<hash> IS removed (positive control)" '[ ! -e "$T/debug/.fingerprint/foo_bar-$H1" ]'
check "underscore pkg: foreign foo-bar-<hash> is untouched" '[ -d "$T/debug/.fingerprint/foo-bar-$H1" ]'

# The pinned repo is the MAIN checkout (parent of git's common dir), never the
# worktree the install ran from, since flow-finalize deletes worktrees.
stable=$(dirname "$(git -C "$REPO_ROOT" rev-parse --path-format=absolute --git-common-dir)")
svc="$tmp/cfg/systemd/user/cqlite-cargo-prune.service"
CARGO_TARGET_DIR="$T" run --install-timer
check "install: exit 0, INSTALLED" '[ "$rc" = 0 ] && grep -q "^RESULT: INSTALLED$" <<<"$out"'
check "install: copied script is executable" '[ -x "$tmp/data/cqlite/cargo-target-prune.sh" ]'
check "install: service pins the main checkout and the target" 'grep -qxF "ExecStart=/usr/bin/env bash $tmp/data/cqlite/cargo-target-prune.sh --repo $stable --target $T" "$svc"'
check "install: service PATH carries cargo's dir" 'grep -qxF "Environment=PATH=$(dirname "$(command -v cargo)"):/usr/local/bin:/usr/bin:/bin" "$svc"'
# Execute the installed command the way systemd would: empty environment plus
# the unit's PATH. Real pgrep/systemctl run here, so inside a gate it SKIPs as
# busy; either way it must resolve cargo and its args, never ERROR.
unit_path=$(sed -n 's/^Environment=PATH=//p' "$svc"); exec_cmd=$(sed -n 's/^ExecStart=//p' "$svc")
read -ra exec_argv <<<"$exec_cmd"
out=$(env -i HOME="$HOME" PATH="$unit_path" CQLITE_PRUNE_PACKAGES="$CQLITE_PRUNE_PACKAGES" "${exec_argv[@]}" 2>&1); rc=$?
check "installed command runs under a minimal env (PRUNED or SKIPPED-BUSY)" '[ "$rc" = 0 ] && grep -qE "^RESULT: (PRUNED|SKIPPED-BUSY)$" <<<"$out"'
check "install: timer is weekly and persistent" 'grep -q "^OnCalendar=Sun" "$tmp/cfg/systemd/user/cqlite-cargo-prune.timer" && grep -q "^Persistent=true" "$tmp/cfg/systemd/user/cqlite-cargo-prune.timer"'
check "install: enabled the timer" 'grep -q "enable --now cqlite-cargo-prune.timer" "$tmp/systemctl.log"'
rel=$(realpath --relative-to="$REPO_ROOT" "$T")
(cd "$REPO_ROOT" && CARGO_TARGET_DIR="$rel" bash "$SCRIPT" --repo "$REPO_ROOT" --install-timer >/dev/null 2>&1)
check "install: a relative target is pinned as an absolute path" 'grep -q -- "--target $(cd "$T" && pwd -P)\$" "$svc"'
rm -f "$svc"; mkdir -p "$tmp/sp ace/t"; : >"$tmp/sp ace/t/CACHEDIR.TAG"
CARGO_TARGET_DIR="$tmp/sp ace/t" run --install-timer
check "install: a target path with a space is refused, no unit written" '[ "$rc" = 1 ] && grep -q "^RESULT: ERROR$" <<<"$out" && [ ! -e "$svc" ]'
mkdir -p "$tmp/pct%n"; : >"$tmp/pct%n/CACHEDIR.TAG"
CARGO_TARGET_DIR="$tmp/pct%n" run --install-timer
check "install: a target path with a systemd % specifier is refused" '[ "$rc" = 1 ] && [ ! -e "$svc" ]'
rm -f "$svc" "${svc%.service}.timer"
for bad in 'bs\dir' 'bs\x2fdir' 'dq"dir' "sq'dir" 'dl$dir'; do
  mkdir -p "$tmp/$bad"; : >"$tmp/$bad/CACHEDIR.TAG"
  CARGO_TARGET_DIR="$tmp/$bad" run --install-timer
  check "install: a target path containing [$bad] is refused (exit 1, ERROR), no unit written" '[ "$rc" = 1 ] && grep -q "^RESULT: ERROR$" <<<"$out" && [ ! -e "$svc" ] && [ ! -e "${svc%.service}.timer" ]'
done
check "install: the backslash refusal is its own named error" 'grep -q "containing a backslash" <<<"$(CARGO_TARGET_DIR="$tmp/bs\\x2fdir" bash "$SCRIPT" --repo "$REPO_ROOT" --install-timer 2>&1)"'
CARGO_TARGET_DIR="$T" run --install-timer
check "install: POSITIVE CONTROL - a normal path is still accepted after the refusals" '[ "$rc" = 0 ] && grep -q "^RESULT: INSTALLED$" <<<"$out" && [ -e "$svc" ]'
run --uninstall-timer
check "uninstall: units removed" '[ "$rc" = 0 ] && [ ! -e "$tmp/cfg/systemd/user/cqlite-cargo-prune.timer" ] && [ ! -e "$tmp/cfg/systemd/user/cqlite-cargo-prune.service" ]'

# 7. uninstall verifies the timer stopped before removing unit files.
mkdir -p "$tmp/cfg/systemd/user"; : >"$svc"; : >"${svc%.service}.timer"
shim systemctl-nostop 'case " $* " in *" disable "*) exit 1;; *" is-active "*) echo active; exit 3;; esac; exit 0'
CQLITE_PRUNE_SYSTEMCTL="$tmp/systemctl-nostop" run --uninstall-timer
check "uninstall: disable --now fails and timer still active -> ERROR, unit files kept" '[ "$rc" = 1 ] && grep -q "^RESULT: ERROR$" <<<"$out" && ! grep -q UNINSTALLED <<<"$out" && [ -e "$svc" ] && [ -e "${svc%.service}.timer" ]'
shim systemctl-inactive 'case " $* " in *" disable "*) exit 1;; *" is-active "*) echo inactive; exit 3;; esac; exit 0'
CQLITE_PRUNE_SYSTEMCTL="$tmp/systemctl-inactive" run --uninstall-timer
check "uninstall: disable fails but is-active=inactive -> UNINSTALLED, files removed" '[ "$rc" = 0 ] && grep -q "^RESULT: UNINSTALLED$" <<<"$out" && [ ! -e "$svc" ] && [ ! -e "${svc%.service}.timer" ]'

# #4387: per-worktree dirs. A FRESH base made by the gate helper has no cargo CACHEDIR.TAG of its
# own; the helper writes one, so the prune accepts the base and can remove an orphaned wt dir.
HELPER="$REPO_ROOT/scripts/lib/worktree-target-dir.sh"
base="$tmp/fresh-base"; live="$tmp/live-wt"; gone="$tmp/gone-wt"
mkdir -p "$live" "$gone"; : >"$live/.git"; : >"$gone/.git"
dl=$(CARGO_TARGET_DIR="$base" bash "$HELPER" "$live"); dg=$(CARGO_TARGET_DIR="$base" bash "$HELPER" "$gone")
rm -rf "$gone"
run --target "$base"
check "fresh base (no cargo CACHEDIR.TAG of its own) still prunes an orphaned wt dir" '[ "$rc" = 0 ] && grep -q "orphan-worktree-dirs-removed=1" <<<"$out" && [ ! -d "$dg" ]'
check "a live worktree's wt dir is kept" '[ -d "$dl" ] && [ -f "$dl/.worktree-root" ]'
run --target "$dl" --dry-run
check "a marked wt/<hash> target is widened to the shared base" 'grep -q "target=$(cd "$base" && pwd -P) " <<<"$out"'

# A hand-configured target that merely ends in wt/<16 hex> has no marker: never widened.
mk_target; manual="$T/wt/0123456789abcdef"; mkdir -p "$manual"; cp "$T/CACHEDIR.TAG" "$manual/CACHEDIR.TAG"
mkdir -p "$manual/debug/.fingerprint"; : >"$manual/debug/.cargo-lock"
run --target "$manual" --dry-run
check "an unmarked .../wt/<16 hex> target is NOT widened to its parent" 'grep -q "target=$(cd "$manual" && pwd -P) " <<<"$out" && ! grep -q "profiles=2" <<<"$out"'

echo "PASS=$PASS FAIL=$FAIL SKIP=0"
[ "$FAIL" = 0 ]
