#!/usr/bin/env bash
# test_install_noble_git.sh — #4510: scripts/ci/install-noble-git.sh survives apt mirror failures.
#
# HERMETIC: apt-get, apt-cache, sudo and git are all stubbed; nothing touches the host. Covers a clean
# install, retry-then-success (update and install), the fallback to a second source, the final
# `INFRA:` failure (distinct exit 75), and the genuine non-infra failures that must NOT read as INFRA.
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd -P)"
SUT="$REPO_ROOT/scripts/ci/install-noble-git.sh"
PASS=0; FAIL=0
ok()  { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad() { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }
[ -f "$SUT" ] || { echo "FAIL - missing $SUT"; exit 1; }
T=$(mktemp -d "${TMPDIR:-/tmp}/install-noble-git.XXXXXX") || exit 1
case "$T" in /?*) ;; *) echo "FAIL - sandbox path not absolute"; exit 1 ;; esac
trap 'rm -rf "$T"' EXIT

cat > "$T/apt-get" <<'STUB'
#!/usr/bin/env bash
args="$*"; echo "APT-GET $args" >> "$STUB_LOG"
fb=primary; fbdir=""
case "$args" in *Dir::Etc::sourceparts=*) fb=fallback; fbdir="$(sed -E 's/.*Dir::Etc::sourceparts=([^ ]+).*/\1/' <<<"$args")" ;; esac
case "$args" in *" update"*) act=update ;; *" install "*) act=install ;; *) act=other ;; esac
cnt="$STUB_DIR/$act.$fb"; n=$(( $(cat "$cnt" 2>/dev/null || echo 0) + 1 )); echo "$n" > "$cnt"
if [ "$fb" = fallback ] && [ -f "$fbdir/fallback.sources" ]; then sed 's/^/FALLBACK-SOURCE /' "$fbdir/fallback.sources" >> "$STUB_LOG"; fi
case "$act.$fb" in
  update.primary) lim="${STUB_UPDATE_FAILS_PRIMARY:-0}" ;;  update.fallback) lim="${STUB_UPDATE_FAILS_FALLBACK:-0}" ;;
  install.primary) lim="${STUB_INSTALL_FAILS_PRIMARY:-0}" ;; install.fallback) lim="${STUB_INSTALL_FAILS_FALLBACK:-0}" ;;
  *) lim=0 ;;
esac
if [ "$n" -le "$lim" ]; then
  echo "W: Failed to fetch http://azure.archive.ubuntu.com/ubuntu/dists/noble/InRelease  Temporary failure resolving 'azure.archive.ubuntu.com'" >&2
  exit 100
fi
exit 0
STUB
cat > "$T/apt-cache" <<'STUB'
#!/usr/bin/env bash
[ -z "${STUB_NO_243:-}" ] || { echo " git | 1:2.50.0-1 | http://example/ noble/main amd64 Packages"; exit 0; }
echo " git | 1:2.43.0-1ubuntu7.4 | http://azure.archive.ubuntu.com/ubuntu noble-updates/main amd64 Packages"
echo " git | 1:2.43.0-1ubuntu7 | http://azure.archive.ubuntu.com/ubuntu noble/main amd64 Packages"
STUB
chmod +x "$T/apt-get" "$T/apt-cache"
printf 'VERSION_CODENAME=noble\n' > "$T/os-noble"; printf 'VERSION_CODENAME=jammy\n' > "$T/os-jammy"

# run <label> [env assignments...] -> $OUT $RC; counters in $T/d-<label>
run() {
  local label="$1"; shift
  STUB_DIR="$T/d-$label"; STUB_LOG="$T/log-$label"; mkdir -p "$STUB_DIR"; : > "$STUB_LOG"
  OUT=$( env APT_GET="$T/apt-get" APT_CACHE="$T/apt-cache" INSTALL_GIT_BACKOFF=0 INSTALL_GIT_RETRIES=3 \
         OS_RELEASE_FILE="$T/os-noble" GIT_VERSION_CMD="echo git version 2.43.0" \
         STUB_DIR="$STUB_DIR" STUB_LOG="$STUB_LOG" "$@" bash "$SUT" 2>&1 )
  RC=$?
  LOG="$STUB_LOG"; DIR="$STUB_DIR"
}
cnt() { cat "$DIR/$1" 2>/dev/null || echo 0; }
has() { grep -q -- "$1" <<<"$OUT"; }

# 1. clean
run clean
[ "$RC" = 0 ] && [ "$(cnt update.primary)" = 1 ] && [ "$(cnt install.primary)" = 1 ] && has "pinned git: git version 2.43.0" \
  && ok "clean run: one update, one install, pinned" || bad "clean run: rc=$RC $OUT"
grep -q 'Acquire::Retries=5' "$LOG" && ok "apt is invoked with Acquire::Retries" || bad "no Acquire::Retries in: $(cat "$LOG")"

# 2. update fails twice then succeeds
run upd STUB_UPDATE_FAILS_PRIMARY=2
{ [ "$RC" = 0 ] && [ "$(cnt update.primary)" = 3 ] && [ "$(cnt update.fallback)" = 0 ] && has "retrying in"; } \
  && ok "retry-then-success: update failed twice, succeeded on attempt 3, no fallback used" || bad "update retry: rc=$RC updates=$(cnt update.primary) $OUT"

# 3. install fails once then succeeds
run ins STUB_INSTALL_FAILS_PRIMARY=1
{ [ "$RC" = 0 ] && [ "$(cnt install.primary)" = 2 ]; } && ok "retry-then-success: install retried once" || bad "install retry: rc=$RC $OUT"

# 4. the configured mirror fails persistently -> second source
run fb STUB_UPDATE_FAILS_PRIMARY=99
{ [ "$RC" = 0 ] && [ "$(cnt update.primary)" = 3 ] && [ "$(cnt update.fallback)" = 1 ] && has "retrying once against http://archive.ubuntu.com/ubuntu"; } \
  && ok "persistent mirror failure: 3 bounded attempts, then ONE pass against the second source, which succeeds" || bad "fallback: rc=$RC $OUT"
grep -q 'FALLBACK-SOURCE URIs: http://archive.ubuntu.com/ubuntu' "$LOG" && ok "the fallback apt source points at archive.ubuntu.com" || bad "fallback source not archive.ubuntu.com: $(grep FALLBACK "$LOG")"
grep -q 'Dir::Etc::sourcelist=/dev/null' "$LOG" && ok "the fallback replaces, rather than edits, the runner's apt sources" || bad "fallback does not isolate the apt sources"

# 5. install persistently fails on the primary -> fallback pass runs update + install again
run fbi STUB_INSTALL_FAILS_PRIMARY=99
{ [ "$RC" = 0 ] && [ "$(cnt install.fallback)" = 1 ]; } && ok "persistent install failure also falls back" || bad "install fallback: rc=$RC $OUT"

# 6. everything fails -> INFRA, distinct exit status
run infra STUB_UPDATE_FAILS_PRIMARY=99 STUB_UPDATE_FAILS_FALLBACK=99
{ [ "$RC" = 75 ] && has "INFRA:" && has "::error::INFRA:"; } && ok "final failure is INFRA: with exit 75 (never a plain test-style failure)" || bad "infra: rc=$RC $OUT"
[ "$(cnt update.primary)" = 3 ] && [ "$(cnt update.fallback)" = 3 ] && ok "the retry loops are bounded (3 + 3 attempts)" || bad "unbounded or wrong attempt counts: $(cnt update.primary)/$(cnt update.fallback)"

# 7. genuine non-infra failures must NOT read as INFRA
run no243 STUB_NO_243=1
{ [ "$RC" = 1 ] && has "no 2.43 git" && ! has "INFRA:"; } && ok "no 2.43 in the archive: plain error, not INFRA" || bad "no-2.43: rc=$RC $OUT"
run wrong "GIT_VERSION_CMD=echo git version 2.50.0"
{ [ "$RC" = 1 ] && has "wanted 2.43" && ! has "INFRA:"; } && ok "wrong git after the pin: plain error, not INFRA" || bad "wrong git: rc=$RC $OUT"
run jammy "OS_RELEASE_FILE=$T/os-jammy"
{ [ "$RC" = 1 ] && has "not Ubuntu noble" && ! has "INFRA:"; } && ok "not noble: plain error, not INFRA" || bad "not noble: rc=$RC $OUT"

echo "test_install_noble_git: pass=$PASS fail=$FAIL"
[ "$FAIL" = 0 ]
