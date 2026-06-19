#!/usr/bin/env bash
#
# bootstrap-cassandra.sh — fetch and build the pinned Apache Cassandra source so
# the compaction-parity harness can compile against its test-tree classes
# (CQLTester, CompactionTask, JsonTransformer) which are NOT published to Maven.
#
# The version is pinned to the SAME release as the cassandra:5.0.2 Docker image
# used to generate test data (single source of truth, issue #669). Build it once;
# subsequent runs are a no-op unless --force is passed.
#
# Cassandra 5.0 builds with JDK 11 or 17 (NOT 21). This script picks a JDK 11
# from $CASSANDRA_BUILD_JAVA_HOME, else an sdkman-installed JDK 11, else errors
# with guidance. The harness itself (Gradle) runs on the default JDK.
#
# Output: prints the built source directory on the last line (stdout) so callers
# (Gradle) can locate build/classes/main, build/test/classes and the dep jars.

set -euo pipefail

CASSANDRA_REF="${CASSANDRA_REF:-cassandra-5.0.2}"
CACHE_ROOT="${CQLITE_CASSANDRA_CACHE:-$HOME/.cache/cqlite/cassandra-src}"
SRC_DIR="$CACHE_ROOT/$CASSANDRA_REF"
MARKER="$SRC_DIR/.cqlite-build-complete"
FORCE=0

for arg in "$@"; do
    case "$arg" in
        --force) FORCE=1 ;;
        *) echo "unknown argument: $arg" >&2; exit 2 ;;
    esac
done

log() { echo "[bootstrap-cassandra] $*" >&2; }

if [[ "$FORCE" -eq 0 && -f "$MARKER" ]]; then
    log "already built at $SRC_DIR (pass --force to rebuild)"
    echo "$SRC_DIR"
    exit 0
fi

# ── Select a JDK 11/17 for the Cassandra build ──────────────────────────────
pick_build_jdk() {
    if [[ -n "${CASSANDRA_BUILD_JAVA_HOME:-}" ]]; then
        echo "$CASSANDRA_BUILD_JAVA_HOME"
        return
    fi
    # Prefer an sdkman-installed JDK 11, then 17 (newest patch of each).
    local candidates_dir="$HOME/.sdkman/candidates/java"
    local major chosen="" path
    for major in 11 17; do
        chosen=""
        for path in "$candidates_dir/$major."*; do
            [[ -x "$path/bin/javac" ]] && chosen="$path"
        done
        if [[ -n "$chosen" ]]; then
            echo "$chosen"
            return
        fi
    done
    echo ""
}

BUILD_JAVA_HOME="$(pick_build_jdk)"
if [[ -z "$BUILD_JAVA_HOME" || ! -x "$BUILD_JAVA_HOME/bin/javac" ]]; then
    log "ERROR: no JDK 11/17 found for the Cassandra build."
    log "Set CASSANDRA_BUILD_JAVA_HOME to a JDK 11 (or 17) home, e.g.:"
    log "  sdk install java 11.0.25-amzn && CASSANDRA_BUILD_JAVA_HOME=\$HOME/.sdkman/candidates/java/11.0.25-amzn $0"
    exit 1
fi
log "using build JDK: $BUILD_JAVA_HOME"

command -v ant >/dev/null 2>&1 || { log "ERROR: ant not found on PATH"; exit 1; }

# ── Fetch the pinned source (shallow, single tag) ───────────────────────────
mkdir -p "$CACHE_ROOT"
if [[ ! -d "$SRC_DIR/.git" ]]; then
    log "cloning apache/cassandra @ $CASSANDRA_REF (shallow) → $SRC_DIR"
    rm -rf "$SRC_DIR"
    git clone --depth 1 --branch "$CASSANDRA_REF" \
        https://github.com/apache/cassandra.git "$SRC_DIR"
else
    log "reusing existing checkout at $SRC_DIR"
fi

# ── Build main + test classes + resolve dependency jars ─────────────────────
log "building Cassandra (ant jar build-test) — this takes several minutes"
(
    cd "$SRC_DIR"
    export JAVA_HOME="$BUILD_JAVA_HOME"
    export PATH="$BUILD_JAVA_HOME/bin:$PATH"
    # `jar` compiles main + resolves runtime deps into build/lib/jars and lib/.
    # `build-test` compiles the test tree (CQLTester et al.) into build/test/classes.
    ant -quiet jar
    ant -quiet build-test
)

touch "$MARKER"
log "build complete"
echo "$SRC_DIR"
