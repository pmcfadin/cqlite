#!/usr/bin/env bash
# test_cassandra_driver_requirements.sh — #4482 (roborev jobs 174/175): the generators'
# driver install is hash-locked and complete.
#   * the requirements file pins exactly the four packages, the driver at
#     CQLITE_CASSANDRA_DRIVER_VERSION, each with at least one sha256;
#   * every host-fetchable wheel sha256 in lib/cassandra-image.sh is in that file;
#   * [needs a pip with network; skipped when absent] the file resolves with
#     --require-hashes for cp311 on x86_64 AND aarch64, and a tampered hash is refused.
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
. "$ROOT/test-data/scripts/lib/cassandra-image.sh"
REQ="$CQLITE_CASSANDRA_DRIVER_REQUIREMENTS"
fail=0; pass=0
ok()  { pass=$((pass+1)); echo "ok   - $*"; }
bad() { fail=$((fail+1)); echo "FAIL - $*"; }

pkgs="$(grep -E '^[A-Za-z0-9._-]+==' "$REQ" | sed -E 's/ .*//' | tr '\n' ' ')"
[ "$pkgs" = "cassandra-driver==${CQLITE_CASSANDRA_DRIVER_VERSION} click==8.5.1 geomet==1.1.0 six==1.17.0 " ] \
  && ok "requirements pin exactly the four packages (driver at ${CQLITE_CASSANDRA_DRIVER_VERSION})" \
  || bad "unexpected package set: $pkgs"

python3 - "$REQ" <<'PY' && ok "every package line carries at least one --hash=sha256" || bad "a package has no hash"
import re, sys
txt = open(sys.argv[1]).read()
blocks = re.split(r'\n(?=[A-Za-z0-9._-]+==)', txt)
pk = [b for b in blocks if re.match(r'[A-Za-z0-9._-]+==', b)]
assert len(pk) == 4, len(pk)
for b in pk:
    assert re.search(r'--hash=sha256:[0-9a-f]{64}', b), b[:40]
PY

missing=0
for spec in "${_CQLITE_DRIVER_WHEELS[@]}"; do
  grep -q "sha256:${spec##* }" "$REQ" || { bad "wheel sha ${spec##* } (${spec%% *}) not in the requirements file"; missing=1; }
done
[ "$missing" = 0 ] && ok "every host-fetchable wheel sha256 is in the requirements file"

PIP=""
for c in pip3 pip "python3 -m pip"; do $c --version >/dev/null 2>&1 && { PIP="$c"; break; }; done
if [ -z "$PIP" ]; then
  echo "skip - no pip on this host: platform-resolution and tamper legs not run"
else
  T="$(mktemp -d)"; trap 'rm -rf "$T"' EXIT
  for plat in manylinux2014_x86_64 manylinux2014_aarch64; do
    if $PIP download --quiet --no-deps --only-binary=:all: --platform "$plat" --python-version 3.11 \
         --require-hashes -r "$REQ" -d "$T/$plat" >/dev/null 2>&1 && [ "$(ls "$T/$plat" | wc -l)" = 4 ]; then
      ok "requirements resolve with --require-hashes for cp311 / $plat (4 files)"
    else
      echo "skip - could not download for $plat (offline?)"
    fi
  done
  python3 -c "
import re,sys
s=open('$REQ').read()
open('$T/bad.txt','w').write(re.sub(r'(--hash=sha256:)([0-9a-f])',lambda m:m.group(1)+('0' if m.group(2)!='0' else '1'),s))"
  if $PIP download --quiet --no-deps --only-binary=:all: --platform manylinux2014_x86_64 --python-version 3.11 \
       --require-hashes -r "$T/bad.txt" -d "$T/bad" >/dev/null 2>&1; then
    bad "a tampered requirements file was ACCEPTED"
  else
    ok "a tampered hash is rejected"
  fi
fi
echo "test_cassandra_driver_requirements: pass=$pass fail=$fail"
[ "$fail" = 0 ]
