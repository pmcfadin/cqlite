#!/usr/bin/env bash
# cassandra_image_lib_smoke.sh — self-contained smoke of test-data/scripts/lib/cassandra-image.sh
# that needs ONLY bash + coreutils/sed/grep (no python, no network, no docker), so it runs
# unchanged under macOS's bash 3.2 (`docker run --rm -v "$PWD":/w bash:3.2 bash
# /w/scripts/tests/cassandra_image_lib_smoke.sh`). The main guard,
# test_cassandra_driver_requirements.sh, runs it with the host bash.
# Covers: release selection (default, override, reject), the all-four-distributions presence
# check, and the concurrent-safe wheel cache. Prints "smoke: pass=N fail=M".
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
LIB="$ROOT/test-data/scripts/lib/cassandra-image.sh"
pass=0; fail=0
ok()  { pass=$((pass+1)); echo "ok   - $*"; }
bad() { fail=$((fail+1)); echo "FAIL - $*"; }
echo "bash ${BASH_VERSION}"

# --- release selection --------------------------------------------------------------------
sel() { CQLITE_CASSANDRA_RELEASE="$1" bash -c 'set -e; . "$0"; echo "$CQLITE_CASSANDRA_VERSION $CQLITE_CASSANDRA_REF $CQLITE_CASSANDRA_IMAGE"' "$LIB" 2>&1; }
[ "$(sel "")" = "5.0.8 cassandra-5.0.8 cassandra:5.0.8" ] && ok "default release is the 5.0.8 tuple" || bad "default: $(sel "")"
[ "$(sel 5.0.2)" = "5.0.2 cassandra-5.0.2 cassandra:5.0.2" ] && ok "override selects the 5.0.2 tuple" || bad "override: $(sel 5.0.2)"
out="$(sel 5.0.9)"; case "$out" in *"not a declared release"*) ok "an unknown release is rejected by name";; *) bad "5.0.9: $out";; esac

# shellcheck source=/dev/null
. "$LIB"

# --- presence: ALL FOUR distributions at their pinned versions ----------------------------
T="$(mktemp -d)"; trap 'rm -rf "$T"' EXIT
cat > "$T/engine" <<'ENG'
#!/bin/sh
# fake engine: `exec <c> python3 -c <script> pin...` succeeds iff every pin is in $FAKE_INSTALLED
shift 5
for p in "$@"; do grep -qxF "$p" "$FAKE_INSTALLED" || exit 1; done
exit 0
ENG
chmod +x "$T/engine"
all="$(grep -E '^[A-Za-z0-9._-]+==' "$CQLITE_CASSANDRA_DRIVER_REQUIREMENTS" | sed -E 's/[[:space:]\\].*$//')"
printf '%s\n' "$all" > "$T/all"
[ "$(wc -l < "$T/all" | tr -d ' ')" = 4 ] && ok "the requirements file pins four distributions" || bad "pins: $(cat "$T/all")"
FAKE_INSTALLED="$T/all" _cqlite_driver_present "$T/engine" c && ok "all four exact: present" || bad "all four exact reported absent"
printf 'cassandra-driver==%s\n' "$CQLITE_CASSANDRA_DRIVER_VERSION" > "$T/driver-only"
FAKE_INSTALLED="$T/driver-only" _cqlite_driver_present "$T/engine" c && bad "driver-only reported present" || ok "driver-only: absent (install must run)"
sed 's/^click==.*/click==0.0.1/' "$T/all" > "$T/wrong-click"
FAKE_INSTALLED="$T/wrong-click" _cqlite_driver_present "$T/engine" c && bad "wrong click reported present" || ok "wrong click version: absent"
printf '#!/bin/sh\nexec "$@"\n' > "$T/sudo"; chmod +x "$T/sudo"
FAKE_INSTALLED="$T/all" _cqlite_driver_present "$T/sudo $T/engine" c && ok "a multi-word engine is split" || bad "multi-word engine failed"

# --- wheel cache --------------------------------------------------------------------------
mkdir -p "$T/cache" "$T/src"
printf 'wheel-bytes-a' > "$T/src/a-1.0-py3-none-any.whl"
shaA="$(_cqlite_sha256 "$T/src/a-1.0-py3-none-any.whl")"
printf '#!/bin/sh\nsleep 1\ncp "%s/${1##*/}" "$3"\n' "$T/src" > "$T/fetch"; chmod +x "$T/fetch"
pids=""
for i in 1 2 3 4; do
  ( CQLITE_DRIVER_FETCH="$T/fetch" _cqlite_cached_wheel "$T/cache" "fake://x/a-1.0-py3-none-any.whl" "$shaA" ) &
  pids="$pids $!"
done
rc=0; for p in $pids; do wait "$p" || rc=$((rc+1)); done
[ "$rc" = 0 ] && ok "4 parallel fetches succeed" || bad "$rc parallel fetches failed"
[ "$(ls -A "$T/cache")" = "a-1.0-py3-none-any.whl" ] && ok "the cache holds exactly the verified wheel" || bad "cache: $(ls -A "$T/cache")"
printf garbage > "$T/cache/a-1.0-py3-none-any.whl"
CQLITE_DRIVER_FETCH="$T/fetch" _cqlite_cached_wheel "$T/cache" "fake://x/a-1.0-py3-none-any.whl" "$shaA" \
  && [ "$(_cqlite_sha256 "$T/cache/a-1.0-py3-none-any.whl")" = "$shaA" ] && ok "a corrupted cached wheel is replaced" || bad "corrupted wheel kept"

echo "smoke: pass=$pass fail=$fail"
[ "$fail" = 0 ]
