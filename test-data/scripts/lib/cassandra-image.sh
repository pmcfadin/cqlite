# shellcheck shell=bash
# cassandra-image.sh — the ONE place that names the Cassandra release every fixture
# generator runs against, and how the Python driver reaches the generator container
# (issue #4482 stage 2, owner decision 2026-11-02: Cassandra 5.0.8 is canonical).
#
# Source it; do not execute it:
#   . "$(dirname "${BASH_SOURCE[0]}")/lib/cassandra-image.sh"
#   CASSANDRA_IMAGE="$CQLITE_CASSANDRA_IMAGE"
#
# Committed fixtures and oracles were generated with cassandra:5.0.2 and stay valid
# until they are regenerated; each fixture's own provenance (manifest
# `evidence.cassandra_version`) declares which release produced it. Changing the
# release is a ONE-LINE change here plus the matching manifest `cassandra_source`
# entry (the corpus audit's provenance check fails when they diverge).

CQLITE_CASSANDRA_VERSION="5.0.8"
CQLITE_CASSANDRA_REF="cassandra-${CQLITE_CASSANDRA_VERSION}"
# Commit the `cassandra-5.0.8` tag points at (git rev-parse cassandra-5.0.8^{commit}).
CQLITE_CASSANDRA_GIT_SHA="172227094626d50d4bc9a3d9de2e3659a7844386"
CQLITE_CASSANDRA_IMAGE="cassandra:${CQLITE_CASSANDRA_VERSION}"

# The Python driver the generators insert rows with. PINNED: an unpinned
# `pip3 install cassandra-driver` made two runs of the same generator differ by
# whatever PyPI served that day.
CQLITE_CASSANDRA_DRIVER_VERSION="3.30.1"
_CQLITE_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Hash-locked requirements: every published file's sha256 for each of the 4 pinned
# packages (see the file's header for provenance). The ONLY source of install truth.
CQLITE_CASSANDRA_DRIVER_REQUIREMENTS="${_CQLITE_LIB_DIR}/cassandra-driver-requirements.txt"

# Host-fetchable wheels for the platforms the stock cassandra image ships on (python3.11 on
# x86_64 and on aarch64): used (1) to install into a container with pip but no network and
# (2) to unpack into a container with NO pip. The pure-python wheels are shared; only the
# driver wheel is per architecture, chosen by the container's `uname -m`. Each sha256 below
# must appear in cassandra-driver-requirements.txt (scripts/tests/
# test_cassandra_driver_requirements.sh checks it). Format: "<url> <sha256>".
_CQLITE_DRIVER_WHEELS_COMMON=(
  "https://files.pythonhosted.org/packages/af/90/3bc780df088d439714af8295196a4332a26559ae66fd99865e36f92efa9e/geomet-1.1.0-py3-none-any.whl 4372fe4e286a34acc6f2e9308284850bd8c4aa5bc12065e2abbd4995900db12f"
  "https://files.pythonhosted.org/packages/31/7d/164b7ffc8624fe9efb4221bf9af485d5c1e41b9ae118045d2b6dc5457d8b/click-8.5.1-py3-none-any.whl 8bb5376d2006950386f8c6427613334a2acadc5f559cb80e638d00b4df957e37"
  "https://files.pythonhosted.org/packages/b7/ce/149a00dd41f10bc29e5921b496af8b574d8413afcd5e30dfa0ed46c2cc5e/six-1.17.0-py2.py3-none-any.whl 4721f391ed90541fddacab5acf947aa0d3dc7d27b2e1e8eda2be8970586c3274"
)
_CQLITE_DRIVER_WHEEL_x86_64="https://files.pythonhosted.org/packages/0e/68/dd77df0e56a275f763ee38598c21abc4fd585b8319dc38eaee398433c231/cassandra_driver-3.30.1-cp311-cp311-manylinux2014_x86_64.manylinux_2_17_x86_64.manylinux_2_28_x86_64.whl 6c5ae0218e52944f92ddde07a03cfa941fa4a10097d501a18cfa1b525c92d013"
_CQLITE_DRIVER_WHEEL_aarch64="https://files.pythonhosted.org/packages/01/28/2cef7840b2065d6053ab46347a6f9e149719e4980937df3ec7bca75125ca/cassandra_driver-3.30.1-cp311-cp311-manylinux2014_aarch64.manylinux_2_17_aarch64.manylinux_2_28_aarch64.whl 187bd1457e414bc019a0635603ea0001baada914942198ed23993198e44dec74"
# Every wheel in the table, for the guard test.
_CQLITE_DRIVER_WHEELS=("${_CQLITE_DRIVER_WHEELS_COMMON[@]}" "$_CQLITE_DRIVER_WHEEL_x86_64" "$_CQLITE_DRIVER_WHEEL_aarch64")

# Portable sha256 of a file (HOST side): sha256sum, else shasum -a 256 (macOS), else python3.
_cqlite_sha256() {
  if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | cut -d' ' -f1
  elif command -v shasum >/dev/null 2>&1; then shasum -a 256 "$1" | cut -d' ' -f1
  else python3 -c 'import hashlib, sys; print(hashlib.sha256(open(sys.argv[1], "rb").read()).hexdigest())' "$1"
  fi
}

# True when the container already has ALL FOUR hash-locked distributions at their exact
# pinned versions. The (name, version) pairs are read from the requirements file, so there
# is no second copy of the pins. `$1` is the engine command, possibly several words
# (`sudo -n docker`), split once here.
_cqlite_driver_present() {
  local -a eng pins
  read -r -a eng <<<"$1"
  mapfile -t pins < <(grep -E '^[A-Za-z0-9._-]+==' "$CQLITE_CASSANDRA_DRIVER_REQUIREMENTS" | sed -E 's/[[:space:]\\].*$//')
  [ "${#pins[@]}" -gt 0 ] || return 1
  "${eng[@]}" exec "$2" python3 -c '
import sys
from importlib import metadata
for pin in sys.argv[1:]:
    name, _, want = pin.partition("==")
    try:
        if metadata.version(name) != want:
            sys.exit(1)
    except metadata.PackageNotFoundError:
        sys.exit(1)
import cassandra
' "${pins[@]}" >/dev/null 2>&1
}

# cqlite_install_cassandra_driver <engine> <container>
# Idempotent. Every route installs only packages whose sha256 is listed in
# cassandra-driver-requirements.txt (all four pinned packages, `--no-deps`), so any
# platform pip supports gets the exact pinned set:
#   1. container has pip3: `pip3 install --no-deps --require-hashes -r <requirements>`
#      from the package index (works on any architecture / python version);
#   2. that failed (container offline) and the container is python3.11 on x86_64/aarch64: the same
#      requirements installed `--no-index --find-links` from wheels the HOST downloaded
#      and sha256-verified;
#   3. container has NO pip (the stock cassandra image) and is python3.11 / x86_64: those
#      same verified wheels are unpacked into dist-packages;
#   4. otherwise: a clear error naming route 1.
cqlite_install_cassandra_driver() {
  local engine="$1" container="$2" spec url sha name cache dir pyver arch have_pip=0 verified=0 wheels=()
  local -a eng
  read -r -a eng <<<"$engine"   # ENGINE may be several words, e.g. "sudo -n docker"
  _cqlite_driver_present "$engine" "$container" && return 0

  pyver="$("${eng[@]}" exec "$container" python3 -c 'import sys; print("%d.%d" % sys.version_info[:2])' 2>/dev/null)"
  arch="$("${eng[@]}" exec "$container" uname -m 2>/dev/null)"
  [ "$arch" = "arm64" ] && arch="aarch64"
  if [ "$pyver" = "3.11" ] && [ "$arch" = "x86_64" ]; then
    verified=1; wheels=("$_CQLITE_DRIVER_WHEEL_x86_64" "${_CQLITE_DRIVER_WHEELS_COMMON[@]}")
  elif [ "$pyver" = "3.11" ] && [ "$arch" = "aarch64" ]; then
    verified=1; wheels=("$_CQLITE_DRIVER_WHEEL_aarch64" "${_CQLITE_DRIVER_WHEELS_COMMON[@]}")
  fi
  "${eng[@]}" exec "$container" bash -c 'command -v pip3 >/dev/null 2>&1' >/dev/null 2>&1 && have_pip=1

  if [ "$have_pip" = 0 ] && [ "$verified" = 0 ]; then
    echo "[driver] container has no pip3 and is python ${pyver:-?} on ${arch:-?}: no verified wheels for this platform." >&2
    echo "[driver] use an image with pip3 (route 1: pip3 install --no-deps --require-hashes -r ${CQLITE_CASSANDRA_DRIVER_REQUIREMENTS##*/}) and rerun." >&2
    return 1
  fi

  if [ "$have_pip" = 1 ]; then
    "${eng[@]}" cp "$CQLITE_CASSANDRA_DRIVER_REQUIREMENTS" "$container:/tmp/cqlite-driver-requirements.txt" || return 1
    "${eng[@]}" exec "$container" pip3 install --quiet --no-deps --require-hashes \
        -r /tmp/cqlite-driver-requirements.txt >/dev/null 2>&1 \
      || "${eng[@]}" exec "$container" pip3 install --quiet --no-deps --require-hashes \
        --break-system-packages -r /tmp/cqlite-driver-requirements.txt >/dev/null 2>&1 || true
    _cqlite_driver_present "$engine" "$container" && return 0
    [ "$verified" = 1 ] || {
      echo "[driver] pip3 --require-hashes install failed in $container and no host-verified wheels exist for ${pyver} on ${arch}" >&2
      return 1
    }
  fi

  cache="${CQLITE_DRIVER_WHEEL_CACHE:-${XDG_CACHE_HOME:-$HOME/.cache}/cqlite/driver-wheels}"
  dir="$(mktemp -d)" || return 1
  mkdir -p "$cache" "$dir/wheels"
  for spec in "${wheels[@]}"; do
    url="${spec% *}"; sha="${spec##* }"; name="${url##*/}"
    if ! { [ -f "$cache/$name" ] && [ "$(_cqlite_sha256 "$cache/$name")" = "$sha" ]; }; then
      curl -fsSL "$url" -o "$cache/$name.part" && mv "$cache/$name.part" "$cache/$name" \
        || { echo "[driver] cannot download $name" >&2; rm -rf "$dir"; return 1; }
    fi
    if [ "$(_cqlite_sha256 "$cache/$name")" != "$sha" ]; then
      echo "[driver] sha256 mismatch for $name (expected $sha)" >&2
      rm -f "$cache/$name"; rm -rf "$dir"; return 1
    fi
    cp "$cache/$name" "$dir/wheels/$name"
  done

  if [ "$have_pip" = 1 ]; then
    "${eng[@]}" exec "$container" mkdir -p /tmp/cqlite-driver-wheels \
      && "${eng[@]}" cp "$dir/wheels/." "$container:/tmp/cqlite-driver-wheels/" \
      && { "${eng[@]}" exec "$container" pip3 install --quiet --no-deps --no-index --require-hashes \
             --find-links /tmp/cqlite-driver-wheels -r /tmp/cqlite-driver-requirements.txt >/dev/null 2>&1 \
           || "${eng[@]}" exec "$container" pip3 install --quiet --no-deps --no-index --require-hashes \
             --break-system-packages --find-links /tmp/cqlite-driver-wheels \
             -r /tmp/cqlite-driver-requirements.txt >/dev/null 2>&1; } || true
  else
    mkdir -p "$dir/unpacked"
    for name in "$dir"/wheels/*.whl; do
      python3 -c 'import sys, zipfile; zipfile.ZipFile(sys.argv[1]).extractall(sys.argv[2])' \
        "$name" "$dir/unpacked" || { rm -rf "$dir"; return 1; }
    done
    "${eng[@]}" cp "$dir/unpacked/." "$container:/usr/lib/python3/dist-packages/" || { rm -rf "$dir"; return 1; }
  fi
  rm -rf "$dir"
  _cqlite_driver_present "$engine" "$container" || {
    echo "[driver] pinned cassandra-driver ${CQLITE_CASSANDRA_DRIVER_VERSION} is not importable in $container" >&2
    return 1
  }
}
