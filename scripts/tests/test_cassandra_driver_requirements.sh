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

# --- fake container engine (python): config via env, state in $FE_DIR ------------------------
T="$(mktemp -d)"
cat > "$T/fake_engine.py" <<'PYENG'
import os, shutil, sys
a = sys.argv[1:]
d = os.environ["FAKE_DIR"]
with open(os.path.join(d, "calls.log"), "a") as f:
    f.write(" ".join(a) + "\n")
state = os.path.join(d, "installed")
if os.path.exists(state):
    installed = set(open(state).read().split())
else:
    installed = set(filter(None, os.environ.get("FAKE_PRESENT", "").split(",")))
if a[0] == "exec":
    cmd = a[2:]
    if cmd[0] == "uname":
        print(os.environ["FAKE_ARCH"])
    elif cmd[0] == "bash":
        sys.exit(1)                       # no pip3 in this container
    elif cmd[0] == "python3":
        if any("version_info" in x for x in cmd):
            print("3.11")
        else:                             # presence probe: python3 -c <script> pin...
            sys.exit(0 if all(p in installed for p in cmd[3:]) else 1)
elif a[0] == "cp":
    src = a[1]
    shutil.copytree(src, os.path.join(d, "dist"), dirs_exist_ok=True)
    for e in os.listdir(src):
        if e.endswith(".dist-info"):
            n, v = e[: -len(".dist-info")].rsplit("-", 1)
            installed.add(n.replace("_", "-") + "==" + v)
    open(state, "w").write("\n".join(sorted(installed)))
PYENG
# fe_setup <name> <arch> <comma-list of pins already present>; sets FE_DIR FE_LOG FE_ENGINE
fe_setup() {
  FE_DIR="$T/fe-$1"; mkdir -p "$FE_DIR"; FE_LOG="$FE_DIR/calls.log"; : > "$FE_LOG"
  FE_ENGINE="$FE_DIR/engine"
  printf '#!/bin/sh\nFAKE_DIR="%s" FAKE_ARCH="%s" FAKE_PRESENT="%s" exec python3 "%s/fake_engine.py" "$@"\n' \
    "$FE_DIR" "$2" "$3" "$T" > "$FE_ENGINE"; chmod +x "$FE_ENGINE"
}
# all four exact -> the install is SKIPPED entirely (no download, no copy, no arch probe)
all_pins="$(grep -E '^[A-Za-z0-9._-]+==' "$REQ" | sed -E 's/[[:space:]\\].*$//' | paste -sd, -)"
fe_setup "all-exact" x86_64 "$all_pins"
CQLITE_DRIVER_WHEEL_CACHE="$T/wheel-cache" cqlite_install_cassandra_driver "$FE_ENGINE" fakec >/dev/null 2>&1
if [ "$?" = 0 ] && ! grep -qE "^cp |uname" "$FE_LOG"; then ok "all four distributions at the pinned versions: the install is skipped"; else bad "all-exact container was not skipped: $(cat "$FE_LOG" | head -3 | tr '\n' ';')"; fi

# --- release selection: CQLITE_CASSANDRA_RELEASE picks one COHERENT tuple (#4482) -----------
read -r -d '' PINS_PY <<'PY' || true
import re, sys
txt = open(sys.argv[1]).read()
blk = txt[txt.index("cassandra_source:"):]
blk = blk[: re.search(r"\n\S", blk[len("cassandra_source:"):]).start() + len("cassandra_source:")]
refs = re.findall(r"^\s+(?:-\s+)?ref:\s*(\S+)", blk, re.M)
shas = re.findall(r"^\s+(?:-\s+)?sha:\s*(\S+)", blk, re.M)
for r, s in zip(refs, shas):
    print(r[len("cassandra-"):], s)
PY
manifest_pins="$(python3 -c "$PINS_PY" "$ROOT/test-data/cassandra-parity-manifest.yml" | sort)"
lib_pins="$(printf '%s\n' "${_CQLITE_CASSANDRA_RELEASES[@]}" | sort)"
[ "$manifest_pins" = "$lib_pins" ] && ok "the lib's release table equals the manifest's primary + additional pins" \
  || bad "lib release table != manifest pins: lib=[$lib_pins] manifest=[$manifest_pins]"
primary="$(python3 -c "$PINS_PY" "$ROOT/test-data/cassandra-parity-manifest.yml" | head -1)"
sel() { CQLITE_CASSANDRA_RELEASE="$1" bash -c '. "$0/test-data/scripts/lib/cassandra-image.sh" && echo "$CQLITE_CASSANDRA_VERSION $CQLITE_CASSANDRA_REF $CQLITE_CASSANDRA_GIT_SHA $CQLITE_CASSANDRA_IMAGE"' "$ROOT" 2>/dev/null; }
got="$(sel 5.0.2)"
[ "$got" = "${primary% *} cassandra-${primary% *} ${primary#* } cassandra:${primary% *}" ] \
  && ok "CQLITE_CASSANDRA_RELEASE=5.0.2 selects the coherent primary tuple (version, ref, sha, image)" || bad "5.0.2 override tuple: $got"
got="$(sel "")"
[ "${got%% *}" = "$CQLITE_CASSANDRA_DEFAULT_RELEASE" ] && ok "the default release is $CQLITE_CASSANDRA_DEFAULT_RELEASE" || bad "default tuple: $got"
for badrel in 5.0.9 5.0 cassandra-5.0.2 "5.0.2 "; do
  if out="$(CQLITE_CASSANDRA_RELEASE="$badrel" bash -c 'set -e; . "$0/test-data/scripts/lib/cassandra-image.sh"; echo SOURCED' "$ROOT" 2>&1)" && printf '%s' "$out" | grep -q SOURCED; then
    bad "an unknown release '$badrel' was accepted"
  else
    printf '%s' "$out" | grep -q "not a declared release" && ok "unknown release '$badrel' is rejected by name" || bad "'$badrel' rejected without the message: $out"
  fi
done
# every audited workflow carries the override, with the removal comment
for wf in exhaustive-regeneration parity-regen-matrix; do
  grep -qE '^  CQLITE_CASSANDRA_RELEASE: "5\.0\.2"  # 5\.0\.2 until #4482 fixture-regeneration stage moves fixtures\+evidence together$' "$ROOT/.github/workflows/$wf.yml" \
    && ok "$wf.yml pins CQLITE_CASSANDRA_RELEASE=5.0.2 with the removal comment" || bad "$wf.yml lacks the 5.0.2 override line"
done

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
  for plat in manylinux2014_x86_64 manylinux2014_aarch64; do
    out="$($PIP download --quiet --no-deps --only-binary=:all: --platform "$plat" --python-version 3.11 \
         --require-hashes -r "$REQ" -d "$T/$plat" 2>&1)"; rc=$?
    if [ "$rc" = 0 ] && [ "$(ls "$T/$plat" | wc -l | tr -d ' ')" = 4 ]; then
      ok "requirements resolve with --require-hashes for cp311 / $plat (4 files)"
    else
      bad "requirements did NOT resolve for cp311 / $plat (rc=$rc) with pypi reachable: $(printf '%s' "$out" | tail -2 | tr '\n' ' ')"
    fi
  done
  # --- install paths through the FAKE engine (needs the wheels, so only with pypi reachable)
  for want_arch in aarch64 x86_64; do
    fe_setup "arch-$want_arch" "$want_arch" ""
    CQLITE_DRIVER_WHEEL_CACHE="$T/wheel-cache" cqlite_install_cassandra_driver "$FE_ENGINE" fakec >/dev/null 2>&1 || true
    so="$(find "$FE_DIR/dist" -name '*.so' 2>/dev/null | head -1)"
    mach="$(python3 -c 'import struct,sys; print(struct.unpack_from("<H", open(sys.argv[1],"rb").read(20), 18)[0])' "$so" 2>/dev/null)"
    exp=$([ "$want_arch" = aarch64 ] && echo 183 || echo 62)
    [ "$mach" = "$exp" ] && ok "no-pip unpack picks the $want_arch driver wheel (ELF e_machine $mach)" \
      || bad "no-pip unpack for $want_arch gave ELF e_machine '${mach:-none}' (expected $exp)"
  done
  # a multi-word engine ("sudo -n <engine>") still works: every helper call goes through it
  fe_setup "multiword" aarch64 ""
  printf '#!/bin/sh\n[ "$1" = "-n" ] && shift\necho "$*" >> "%s/sudo.log"\nexec "$@"\n' "$FE_DIR" > "$T/fakesudo"; chmod +x "$T/fakesudo"
  CQLITE_DRIVER_WHEEL_CACHE="$T/wheel-cache" cqlite_install_cassandra_driver "$T/fakesudo -n $FE_ENGINE" fakec >/dev/null 2>&1 || true
  if [ -f "$FE_DIR/sudo.log" ] && grep -q "exec fakec" "$FE_DIR/sudo.log" && grep -q "^cp " "$FE_LOG" && [ -n "$(find "$FE_DIR/dist" -name '*.so' 2>/dev/null | head -1)" ]; then
    ok "a multi-word engine ('sudo -n <engine>') is split once and used for every call ($(wc -l < "$FE_DIR/sudo.log" | tr -d ' ') calls)"
  else
    bad "multi-word engine did not drive the install (sudo.log: $(cat "$FE_DIR/sudo.log" 2>/dev/null | head -2 | tr '\n' ';'))"
  fi
  # driver alone present -> the hash-locked install MUST still run; a wrong click -> runs too
  all_pins="$(grep -E '^[A-Za-z0-9._-]+==' "$REQ" | sed -E 's/[[:space:]\\].*$//' | paste -sd, -)"
  fe_setup "driver-only" x86_64 "cassandra-driver==${CQLITE_CASSANDRA_DRIVER_VERSION}"
  CQLITE_DRIVER_WHEEL_CACHE="$T/wheel-cache" cqlite_install_cassandra_driver "$FE_ENGINE" fakec >/dev/null 2>&1 || true
  grep -q "^cp " "$FE_LOG" && ok "driver present but click/geomet/six missing: the install RUNS" || bad "driver-only container skipped the install"
  fe_setup "wrong-click" x86_64 "$(printf '%s' "$all_pins" | sed 's/click==[0-9.]*/click==8.0.0/')"
  CQLITE_DRIVER_WHEEL_CACHE="$T/wheel-cache" cqlite_install_cassandra_driver "$FE_ENGINE" fakec >/dev/null 2>&1 || true
  grep -q "^cp " "$FE_LOG" && ok "a wrong click version: the install RUNS" || bad "wrong-click container skipped the install"
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
