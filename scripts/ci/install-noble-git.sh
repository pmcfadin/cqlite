#!/usr/bin/env bash
# install-noble-git.sh — pin the runner's git to the Ubuntu noble archive's 2.43 (#4437), surviving
# apt mirror failures (#4510).
#
# Used by .github/actions/dev-box-parity. On 2026-11-11 the pin step failed twice in a row with
# "Temporary failure resolving 'azure.archive.ubuntu.com'" before any test ran, so a runner network
# blip read as a red tooling tier. This script therefore:
#   1. runs `apt-get update` and `apt-get install` with `Acquire::Retries` and a BOUNDED retry loop
#      with exponential backoff around each;
#   2. on a persistent failure of the runner's configured mirror, retries the whole sequence once
#      against a SECOND SOURCE (archive.ubuntu.com, the origin the Azure mirror replicates), used
#      alone through a throw-away sourceparts directory so the runner's own apt config is untouched;
#   3. if that fails too, exits 75 with an `INFRA:` message (also a GitHub `::error::` annotation),
#      so an infrastructure failure is never read as a test failure.
# A genuinely wrong pin (no 2.43 in the archive, the wrong git after install, not noble) is a plain
# error (exit 1), NOT `INFRA:`.
#
# Test seams (all default to the real thing; scripts/tests/test_install_noble_git.sh stubs them):
#   APT_GET, APT_CACHE   command prefixes (default `sudo apt-get` / `apt-cache`)
#   INSTALL_GIT_RETRIES  attempts per apt call (default 4)
#   INSTALL_GIT_BACKOFF  first backoff in seconds, doubled each retry (default 5; 0 in tests)
#   INSTALL_GIT_FALLBACK_URI   second source (default http://archive.ubuntu.com/ubuntu)
#   OS_RELEASE_FILE, GIT_VERSION_CMD
set -uo pipefail

APT_GET="${APT_GET:-sudo apt-get}"
APT_CACHE="${APT_CACHE:-apt-cache}"
RETRIES="${INSTALL_GIT_RETRIES:-4}"
BACKOFF="${INSTALL_GIT_BACKOFF:-5}"
FALLBACK_URI="${INSTALL_GIT_FALLBACK_URI:-http://archive.ubuntu.com/ubuntu}"
OS_RELEASE_FILE="${OS_RELEASE_FILE:-/etc/os-release}"
GIT_VERSION_CMD="${GIT_VERSION_CMD:-git --version}"
APT_OPTS=(-o Acquire::Retries=5 -o Acquire::http::Timeout=20 -o Acquire::https::Timeout=20)
EXTRA_OPTS=()          # set to the fallback-source options for the second pass

plain_error() { echo "::error::$*" >&2; echo "ERROR: $*" >&2; exit 1; }

# with_retries <description> <command...> — up to $RETRIES attempts, exponential backoff between them.
with_retries() {
  local what="$1" attempt=1 delay="$BACKOFF"
  shift
  while :; do
    if "$@"; then return 0; fi
    if [ "$attempt" -ge "$RETRIES" ]; then
      echo "apt step '${what}' failed after ${attempt} attempt(s)" >&2
      return 1
    fi
    echo "apt step '${what}' failed (attempt ${attempt}/${RETRIES}); retrying in ${delay}s" >&2
    sleep "$delay"
    delay=$((delay * 2))
    attempt=$((attempt + 1))
  done
}

apt_update()  { # shellcheck disable=SC2086
  ${APT_GET} ${EXTRA_OPTS[@]+"${EXTRA_OPTS[@]}"} "${APT_OPTS[@]}" update -qq; }
apt_install() { # shellcheck disable=SC2086
  ${APT_GET} ${EXTRA_OPTS[@]+"${EXTRA_OPTS[@]}"} "${APT_OPTS[@]}" install -y -qq --allow-downgrades "git=$1" "git-man=$1"; }

# find_version — the newest 1:2.43.x git in the configured sources (empty if none).
find_version() { # shellcheck disable=SC2086
  ${APT_CACHE} ${EXTRA_OPTS[@]+"${EXTRA_OPTS[@]}"} madison git | awk '$3 ~ /^1:2\.43\./ {print $3; exit}'; }

# pass <label> — one full update + select + install sequence. 0 ok; 1 network/apt failure; 2 no 2.43 in the archive.
pass() {
  local label="$1" ver
  echo "apt pass: ${label}"
  with_retries "update (${label})" apt_update || return 1
  ver="$(find_version)"
  if [ -z "$ver" ]; then
    return 2
  fi
  with_retries "install git=${ver} (${label})" apt_install "$ver" || return 1
  return 0
}

echo "runner image git: $(${GIT_VERSION_CMD} 2>&1 || true)"
codename="$(. "${OS_RELEASE_FILE}" && printf '%s' "${VERSION_CODENAME:-}")"
if [ "$codename" != noble ]; then
  plain_error "runner is '${codename:-unknown}', not Ubuntu noble; pin the job to runs-on: ubuntu-24.04"
fi

rc=0; pass "configured mirror" || rc=$?
if [ "$rc" = 1 ]; then
  fb="$(mktemp -d)" || plain_error "could not create a scratch directory for the fallback apt source"
  cat > "$fb/fallback.sources" <<SRC
Types: deb
URIs: ${FALLBACK_URI}
Suites: noble noble-updates noble-security
Components: main universe
Signed-By: /usr/share/keyrings/ubuntu-archive-keyring.gpg
SRC
  EXTRA_OPTS=(-o "Dir::Etc::sourcelist=/dev/null" -o "Dir::Etc::sourceparts=${fb}")
  echo "the configured mirror failed persistently; retrying once against ${FALLBACK_URI}" >&2
  rc=0; pass "fallback ${FALLBACK_URI}" || rc=$?
  rm -rf "$fb"
fi
case "$rc" in
  0) ;;
  1) echo "::error::INFRA: could not fetch git from the configured apt mirror or from ${FALLBACK_URI} after ${RETRIES} attempts each; this is a runner/network failure, not a test failure" >&2
     echo "INFRA: apt unreachable (mirror and ${FALLBACK_URI}); no tests were run against the pinned git" >&2
     exit 75 ;;
  *) echo "::error::no 2.43 git in the apt archive" >&2
     ${APT_CACHE} madison git >&2 || true
     exit 1 ;;
esac

if ! ${GIT_VERSION_CMD} | grep -q '^git version 2\.43\.'; then
  plain_error "git is '$(${GIT_VERSION_CMD})' after the pin, wanted 2.43.x"
fi
echo "pinned git: $(${GIT_VERSION_CMD})"
