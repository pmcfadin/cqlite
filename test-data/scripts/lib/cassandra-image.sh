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

# The COMPLETE, exact driver package set (roborev job 174): cp311 / x86_64 wheels, the
# cassandra image's python3.11 and the dependency closure of cassandra-driver, each with
# its sha256. Both install paths below use exactly these files, so an online run and an
# offline run install identical packages; nothing is resolved from an index. Format:
# "<url> <sha256>".
_CQLITE_DRIVER_WHEELS=(
  "https://files.pythonhosted.org/packages/0e/68/dd77df0e56a275f763ee38598c21abc4fd585b8319dc38eaee398433c231/cassandra_driver-3.30.1-cp311-cp311-manylinux2014_x86_64.manylinux_2_17_x86_64.manylinux_2_28_x86_64.whl 6c5ae0218e52944f92ddde07a03cfa941fa4a10097d501a18cfa1b525c92d013"
  "https://files.pythonhosted.org/packages/af/90/3bc780df088d439714af8295196a4332a26559ae66fd99865e36f92efa9e/geomet-1.1.0-py3-none-any.whl 4372fe4e286a34acc6f2e9308284850bd8c4aa5bc12065e2abbd4995900db12f"
  "https://files.pythonhosted.org/packages/31/7d/164b7ffc8624fe9efb4221bf9af485d5c1e41b9ae118045d2b6dc5457d8b/click-8.5.1-py3-none-any.whl 8bb5376d2006950386f8c6427613334a2acadc5f559cb80e638d00b4df957e37"
  "https://files.pythonhosted.org/packages/b7/ce/149a00dd41f10bc29e5921b496af8b574d8413afcd5e30dfa0ed46c2cc5e/six-1.17.0-py2.py3-none-any.whl 4721f391ed90541fddacab5acf947aa0d3dc7d27b2e1e8eda2be8970586c3274"
)

# True when the container already imports the pinned driver version.
_cqlite_driver_present() {
  "$1" exec "$2" python3 -c \
    "import sys, cassandra; sys.exit(0 if cassandra.__version__ == '${CQLITE_CASSANDRA_DRIVER_VERSION}' else 1)" \
    >/dev/null 2>&1
}

# cqlite_install_cassandra_driver <engine> <container>
# Idempotent. The host downloads (once, cached) and sha256-verifies the exact wheel set
# above, then installs ONLY those files into the container:
#   * container has pip3: `pip3 install --no-deps --no-index --require-hashes` from the
#     copied files (pip re-verifies every hash; no index, no resolver);
#   * container has no pip (the stock cassandra image): the same wheels are unpacked
#     into the interpreter's dist-packages.
# Either way the installed packages are those same four wheels. Returns non-zero (with a
# message) when the host cannot fetch/verify them, the container is not cp311/x86_64, or
# the pinned driver is still not importable.
cqlite_install_cassandra_driver() {
  local engine="$1" container="$2" spec url sha name cache dir pyver arch reqs=""
  _cqlite_driver_present "$engine" "$container" && return 0

  pyver="$("$engine" exec "$container" python3 -c 'import sys; print("%d.%d" % sys.version_info[:2])' 2>/dev/null)"
  arch="$("$engine" exec "$container" uname -m 2>/dev/null)"
  if [ "$pyver" != "3.11" ] || [ "$arch" != "x86_64" ]; then
    echo "[driver] the pinned wheel set is cp311/x86_64; container has python ${pyver:-?} on ${arch:-?}" >&2
    return 1
  fi

  cache="${CQLITE_DRIVER_WHEEL_CACHE:-${XDG_CACHE_HOME:-$HOME/.cache}/cqlite/driver-wheels}"
  dir="$(mktemp -d)" || return 1
  mkdir -p "$cache" "$dir/wheels"
  for spec in "${_CQLITE_DRIVER_WHEELS[@]}"; do
    url="${spec% *}"; sha="${spec##* }"; name="${url##*/}"
    if ! { [ -f "$cache/$name" ] && [ "$(sha256sum "$cache/$name" | cut -d' ' -f1)" = "$sha" ]; }; then
      curl -fsSL "$url" -o "$cache/$name.part" && mv "$cache/$name.part" "$cache/$name" \
        || { echo "[driver] cannot download $name" >&2; rm -rf "$dir"; return 1; }
    fi
    if [ "$(sha256sum "$cache/$name" | cut -d' ' -f1)" != "$sha" ]; then
      echo "[driver] sha256 mismatch for $name (expected $sha)" >&2
      rm -f "$cache/$name"; rm -rf "$dir"; return 1
    fi
    cp "$cache/$name" "$dir/wheels/$name"
    reqs="${reqs}/tmp/cqlite-driver-wheels/${name} --hash=sha256:${sha}"$'\n'
  done
  printf '%s' "$reqs" > "$dir/requirements.txt"

  if "$engine" exec "$container" bash -c 'command -v pip3 >/dev/null 2>&1' >/dev/null 2>&1; then
    "$engine" exec "$container" mkdir -p /tmp/cqlite-driver-wheels \
      && "$engine" cp "$dir/wheels/." "$container:/tmp/cqlite-driver-wheels/" \
      && "$engine" cp "$dir/requirements.txt" "$container:/tmp/cqlite-driver-requirements.txt" \
      && "$engine" exec "$container" pip3 install --quiet --no-deps --no-index --require-hashes \
           -r /tmp/cqlite-driver-requirements.txt >/dev/null 2>&1 \
      || "$engine" exec "$container" pip3 install --quiet --no-deps --no-index --require-hashes \
           --break-system-packages -r /tmp/cqlite-driver-requirements.txt >/dev/null 2>&1 \
      || true
  else
    mkdir -p "$dir/unpacked"
    for name in "$dir"/wheels/*.whl; do
      python3 -c 'import sys, zipfile; zipfile.ZipFile(sys.argv[1]).extractall(sys.argv[2])' \
        "$name" "$dir/unpacked" || { rm -rf "$dir"; return 1; }
    done
    "$engine" cp "$dir/unpacked/." "$container:/usr/lib/python3/dist-packages/" || { rm -rf "$dir"; return 1; }
  fi
  rm -rf "$dir"
  _cqlite_driver_present "$engine" "$container" || {
    echo "[driver] pinned cassandra-driver ${CQLITE_CASSANDRA_DRIVER_VERSION} is not importable in $container" >&2
    return 1
  }
}
