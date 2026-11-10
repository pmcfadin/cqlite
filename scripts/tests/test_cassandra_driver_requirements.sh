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

# --- portable host sha256 helper: every fallback branch yields the real digest -----------
SHA_T="$(mktemp -d)"; trap 'rm -rf "$SHA_T" "${T:-}"' EXIT
printf 'cqlite' > "$SHA_T/f"
want="$(python3 -c 'import hashlib; print(hashlib.sha256(b"cqlite").hexdigest())')"
mkdir -p "$SHA_T/b_sha256sum" "$SHA_T/b_shasum" "$SHA_T/b_python"
for d in b_sha256sum b_shasum b_python; do ln -s "$(command -v cut)" "$SHA_T/$d/cut"; done
printf '#!/bin/sh\nprintf "%%s  %%s\\n" "%s" "$1"\n' "$want" > "$SHA_T/b_sha256sum/sha256sum"
printf '#!/bin/sh\nprintf "%%s  %%s\\n" "%s" "$3"\n' "$want" > "$SHA_T/b_shasum/shasum"
ln -s "$(command -v python3)" "$SHA_T/b_python/python3"
chmod +x "$SHA_T/b_sha256sum/sha256sum" "$SHA_T/b_shasum/shasum"
for branch in b_sha256sum b_shasum b_python; do
  got="$(PATH="$SHA_T/$branch" _cqlite_sha256 "$SHA_T/f")"
  [ "$got" = "$want" ] && ok "_cqlite_sha256 via ${branch#b_} returns the file digest" || bad "_cqlite_sha256 via ${branch#b_}: got '$got'"
done
# the aarch64 + x86_64 driver wheels are both in the table
case "$_CQLITE_DRIVER_WHEEL_aarch64 $_CQLITE_DRIVER_WHEEL_x86_64" in
  *aarch64*x86_64*) ok "driver wheel table carries aarch64 and x86_64";; *) bad "driver wheel table lacks an arch";;
esac

PIP=""
for c in pip3 pip "python3 -m pip"; do $c --version >/dev/null 2>&1 && { PIP="$c"; break; }; done
reach() {  # connectivity probe, separate from the resolver
  if command -v curl >/dev/null 2>&1; then curl -fsS -I --max-time 10 https://pypi.org/simple/six/ >/dev/null 2>&1
  else python3 -c 'import urllib.request; urllib.request.urlopen(urllib.request.Request("https://pypi.org/simple/six/", method="HEAD"), timeout=10)' >/dev/null 2>&1
  fi
}
if [ -z "$PIP" ]; then
  echo "skip - no pip on this host: platform-resolution and tamper legs not run"
elif ! reach; then
  echo "skip - pypi.org unreachable (HEAD https://pypi.org/simple/six/ failed): platform-resolution and tamper legs not run"
else
  # Connectivity is CONFIRMED: from here a resolver or download failure is a FAIL.
  T="$(mktemp -d)"
  for plat in manylinux2014_x86_64 manylinux2014_aarch64; do
    out="$($PIP download --quiet --no-deps --only-binary=:all: --platform "$plat" --python-version 3.11 \
         --require-hashes -r "$REQ" -d "$T/$plat" 2>&1)"; rc=$?
    if [ "$rc" = 0 ] && [ "$(ls "$T/$plat" | wc -l | tr -d ' ')" = 4 ]; then
      ok "requirements resolve with --require-hashes for cp311 / $plat (4 files)"
    else
      bad "requirements did NOT resolve for cp311 / $plat (rc=$rc) with pypi reachable: $(printf '%s' "$out" | tail -2 | tr '\n' ' ')"
    fi
  done
  # Arch selection in the no-pip unpack path, driven through a FAKE engine script that
  # reports a python3.11 / <arch> container with no pip: the unpacked driver extension
  # must be built for that arch (ELF e_machine 183 = aarch64, 62 = x86_64).
  for want_arch in aarch64 x86_64; do
    FAKE="$T/fake-$want_arch"; mkdir -p "$FAKE"
    cat > "$T/engine-$want_arch" <<ENG
#!/usr/bin/env bash
# fake container engine: argv = exec <c> <cmd...> | cp <src> <c>:<dst>
case "\$1" in
  exec)
    case "\$3" in
      uname) echo $want_arch ;;
      bash) exit 1 ;;
      python3)
        if [[ "\$*" == *version_info* ]]; then echo 3.11
        else [ -d "$FAKE/cassandra" ]; fi ;;
    esac ;;
  cp) cp -r "\$2" "$FAKE/" ;;
esac
ENG
    chmod +x "$T/engine-$want_arch"
    CQLITE_DRIVER_WHEEL_CACHE="$T/wheel-cache" cqlite_install_cassandra_driver "$T/engine-$want_arch" fakec >/dev/null 2>&1 || true
    so="$(find "$FAKE/cassandra" -name 'cluster*.so' -o -name 'cython_marshal*.so' 2>/dev/null | head -1)"
    [ -z "$so" ] && so="$(find "$FAKE" -name '*.so' 2>/dev/null | head -1)"
    mach="$(python3 -c 'import struct,sys; print(struct.unpack_from("<H", open(sys.argv[1],"rb").read(20), 18)[0])' "$so" 2>/dev/null)"
    exp=$([ "$want_arch" = aarch64 ] && echo 183 || echo 62)
    [ "$mach" = "$exp" ] && ok "no-pip unpack picks the $want_arch driver wheel (ELF e_machine $mach)" \
      || bad "no-pip unpack for $want_arch produced ELF e_machine '${mach:-none}' (expected $exp) from '${so:-no .so}'"
  done
  python3 -c "
import re,sys
s=open('$REQ').read()
open('$T/bad.txt','w').write(re.sub(r'(--hash=sha256:)([0-9a-f])',lambda m:m.group(1)+('0' if m.group(2)!='0' else '1'),s))"
  out="$($PIP download --quiet --no-deps --only-binary=:all: --platform manylinux2014_x86_64 --python-version 3.11 \
       --require-hashes -r "$T/bad.txt" -d "$T/bad" 2>&1)"; rc=$?
  if [ "$rc" != 0 ] && printf '%s' "$out" | grep -q "DO NOT MATCH THE HASHES"; then
    ok "a tampered hash is rejected with pip's hash-mismatch error"
  elif [ "$rc" = 0 ]; then
    bad "a tampered requirements file was ACCEPTED"
  else
    bad "tampered file failed for the WRONG reason (expected pip's hash mismatch): $(printf '%s' "$out" | tail -2 | tr '\n' ' ')"
  fi
fi
echo "test_cassandra_driver_requirements: pass=$pass fail=$fail"
[ "$fail" = 0 ]
