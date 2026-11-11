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
# reject suites a host does not serve: archive.ubuntu.com has no noble-security; security.ubuntu.com only noble-security
if [ "$fb" = fallback ] && [ "$act" = update ] && [ -f "$fbdir/fallback.sources" ]; then
  uri=""; while IFS= read -r l; do
    case "$l" in
      "URIs: "*) uri="${l#URIs: }" ;;
      "Suites: "*) for su in ${l#Suites: }; do
          case "$uri:$su" in
            *archive.ubuntu.com*:noble|*archive.ubuntu.com*:noble-updates|*security.ubuntu.com*:noble-security) ;;
            *) echo "E: The repository '$uri $su Release' does not have a Release file (404)" >&2; exit 100 ;;
          esac; done ;;
    esac; done < "$fbdir/fallback.sources"
fi
case "$act.$fb" in
  update.primary) lim="${STUB_UPDATE_FAILS_PRIMARY:-0}" ;;  update.fallback) lim="${STUB_UPDATE_FAILS_FALLBACK:-0}" ;;
  install.primary) lim="${STUB_INSTALL_FAILS_PRIMARY:-0}" ;; install.fallback) lim="${STUB_INSTALL_FAILS_FALLBACK:-0}" ;;
  *) lim=0 ;;
esac
if [ "$act" = update ] && [ "$n" -le "${STUB_UPDATE_WARN_PRIMARY:-0}" ] && [ "$fb" = primary ]; then
  echo "W: Failed to fetch http://azure.archive.ubuntu.com/ubuntu/dists/noble/InRelease  Temporary failure resolving 'azure.archive.ubuntu.com'" >&2
  echo "W: Some index files failed to download. They have been ignored, or old ones used instead." >&2
  case "$args" in *APT::Update::Error-Mode=any*) [ -n "${STUB_IGNORE_ERROR_MODE:-}" ] || exit 100 ;; esac
  exit 0     # warn-but-exit-0: what apt does without Error-Mode=any
fi
if [ "$n" -le "$lim" ]; then
  echo "W: Failed to fetch http://azure.archive.ubuntu.com/ubuntu/dists/noble/InRelease  Temporary failure resolving 'azure.archive.ubuntu.com'" >&2
  exit 100
fi
exit 0
STUB
cat > "$T/apt-cache" <<'STUB'
#!/usr/bin/env bash
fbk=primary; case "$*" in *Dir::Etc::sourceparts=*) fbk=fallback ;; esac
c="$STUB_DIR/cache.$fbk"; n=$(( $(cat "$c" 2>/dev/null || echo 0) + 1 )); echo "$n" > "$c"
case "$fbk" in primary) lim="${STUB_CACHE_FAILS_PRIMARY:-0}" ;; *) lim="${STUB_CACHE_FAILS_FALLBACK:-0}" ;; esac
if [ "$n" -le "$lim" ]; then echo "E: apt-cache: could not open the package cache" >&2; exit 100; fi
bar='|'
row() { printf ' git %s %s %s http://example/ubuntu %s amd64 Packages\n' "$bar" "$1" "$bar" "$2"; }
if [ -n "${STUB_NO_243:-}" ]; then row 1:2.50.0-1 noble/main; exit 0; fi
row 1:2.43.0-1ubuntu7.4 noble-updates/main
row 1:2.43.0-1ubuntu7 noble/main
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

# 4b. update WARNS about failed indexes and exits 0 (apt without Error-Mode, or ignoring it): still retried, not read as "no 2.43"
grep -q 'APT::Update::Error-Mode=any' "$T/log-fb" && ok "update is invoked with APT::Update::Error-Mode=any" || bad "no Error-Mode=any on update"
run warn STUB_UPDATE_WARN_PRIMARY=2 STUB_IGNORE_ERROR_MODE=1
{ [ "$RC" = 0 ] && [ "$(cnt update.primary)" = 3 ] && has "treating as a failure" && ! has "no 2.43 git"; } \
  && ok "warn-but-exit-0 update (Error-Mode ignored) is detected from its output, retried, then succeeds" || bad "warn-exit-0: rc=$RC updates=$(cnt update.primary) $OUT"
run warn2 STUB_UPDATE_WARN_PRIMARY=99 STUB_IGNORE_ERROR_MODE=1
{ [ "$RC" = 0 ] && [ "$(cnt update.fallback)" = 1 ]; } && ok "persistent warn-but-exit-0 reaches the fallback instead of reading as a missing git" || bad "warn-exit-0 fallback: rc=$RC $OUT"
run warn3 STUB_UPDATE_WARN_PRIMARY=1
{ [ "$RC" = 0 ] && [ "$(cnt update.primary)" = 2 ]; } && ok "with Error-Mode=any honoured the warned update exits non-zero and is retried" || bad "error-mode path: rc=$RC $OUT"

# 4c. the fallback stanzas only ask each host for suites it serves (the stub rejects anything else)
grep -q 'FALLBACK-SOURCE Suites: noble-security' "$LOG" 2>/dev/null; run fbs STUB_UPDATE_FAILS_PRIMARY=99
{ [ "$RC" = 0 ] && grep -q 'URIs: http://security.ubuntu.com/ubuntu' "$LOG" && grep -q 'Suites: noble-security' "$LOG" && ! grep -q 'Suites: noble noble-updates noble-security' "$LOG"; } \
  && ok "noble-security is requested from security.ubuntu.com in its own stanza, not from archive.ubuntu.com" || bad "fallback suites wrong: $(grep -E 'URIs|Suites' "$LOG")"

# 4d. the version QUERY failing (apt-cache non-zero) is retryable, falls back, and ends INFRA: never "no 2.43"
run qry STUB_CACHE_FAILS_PRIMARY=2
{ [ "$RC" = 0 ] && [ "$(cnt cache.primary)" = 3 ] && [ "$(cnt update.fallback)" = 0 ] && ! has "no 2.43 git"; } \
  && ok "apt-cache failing twice is retried and then succeeds" || bad "query retry: rc=$RC caches=$(cnt cache.primary) $OUT"
run qryfb STUB_CACHE_FAILS_PRIMARY=99
{ [ "$RC" = 0 ] && [ "$(cnt cache.primary)" = 3 ] && [ "$(cnt update.fallback)" = 1 ] && ! has "no 2.43 git"; } \
  && ok "a persistently failing version query falls back to the second source (not read as a missing 2.43)" || bad "query fallback: rc=$RC $OUT"
run qryinfra STUB_CACHE_FAILS_PRIMARY=99 STUB_CACHE_FAILS_FALLBACK=99
{ [ "$RC" = 75 ] && has "INFRA:" && ! has "no 2.43 git" && [ "$(cnt cache.fallback)" = 3 ]; } \
  && ok "a version query that always fails ends as INFRA (exit 75), never as a plain 'no 2.43' error" || bad "query infra: rc=$RC $OUT"

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
