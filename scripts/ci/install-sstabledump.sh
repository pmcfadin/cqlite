#!/bin/bash

# Install sstabledump binary for CQLite development
# This script installs the Cassandra sstabledump tool needed for Issue #38 zero-diff validation

set -euo pipefail

CASSANDRA_VERSION="5.0.2"
INSTALL_DIR="$HOME/.local/bin"
CASSANDRA_HOME="$HOME/.local/cassandra"

echo "🔧 Installing sstabledump for CQLite development"
echo "   Cassandra version: $CASSANDRA_VERSION"
echo "   Install directory: $INSTALL_DIR"

# Create directories
mkdir -p "$INSTALL_DIR"
mkdir -p "$CASSANDRA_HOME"

# Detect OS and architecture
OS=$(uname -s | tr '[:upper:]' '[:lower:]')
ARCH=$(uname -m)

if [[ "$OS" == "darwin" ]]; then
    if [[ "$ARCH" == "arm64" ]]; then
        JAVA_ARCH="aarch64"
    else
        JAVA_ARCH="x64"
    fi
elif [[ "$OS" == "linux" ]]; then
    if [[ "$ARCH" == "x86_64" ]]; then
        JAVA_ARCH="x64"
    elif [[ "$ARCH" == "aarch64" ]]; then
        JAVA_ARCH="aarch64"
    else
        echo "❌ Unsupported architecture: $ARCH"
        exit 1
    fi
else
    echo "❌ Unsupported OS: $OS"
    exit 1
fi

echo "   Platform: $OS-$JAVA_ARCH"

# Check if Java 17+ is available
if command -v java >/dev/null 2>&1; then
    JAVA_VERSION=$(java -version 2>&1 | grep -E 'version "([0-9]+)' | sed -E 's/.*version "([0-9]+).*/\1/' | head -1)
    if [[ "$JAVA_VERSION" -ge 17 ]]; then
        echo "✅ Java $JAVA_VERSION found"
        JAVA_CMD="java"
    else
        echo "⚠️ Java $JAVA_VERSION is too old, need Java 17+"
        INSTALL_JAVA=true
    fi
else
    echo "⚠️ Java not found, will install"
    INSTALL_JAVA=true
fi

# Install Java 17 if needed
if [[ "${INSTALL_JAVA:-false}" == "true" ]]; then
    echo "📦 Installing Java 17..."
    if [[ "$OS" == "darwin" ]]; then
        if command -v brew >/dev/null 2>&1; then
            brew install openjdk@17
            JAVA_CMD="/opt/homebrew/opt/openjdk@17/bin/java"
        else
            echo "❌ Homebrew not found. Please install Java 17 manually:"
            echo "   brew install openjdk@17"
            exit 1
        fi
    elif [[ "$OS" == "linux" ]]; then
        if command -v apt-get >/dev/null 2>&1; then
            sudo apt-get update
            sudo apt-get install -y openjdk-17-jre-headless
            JAVA_CMD="java"
        elif command -v yum >/dev/null 2>&1; then
            sudo yum install -y java-17-openjdk-headless
            JAVA_CMD="java"
        else
            echo "❌ Package manager not found. Please install Java 17 manually"
            exit 1
        fi
    fi
fi

# Download Cassandra if not already present
CASSANDRA_TAR="apache-cassandra-$CASSANDRA_VERSION-bin.tar.gz"
CASSANDRA_URL="https://archive.apache.org/dist/cassandra/$CASSANDRA_VERSION/$CASSANDRA_TAR"

if [[ ! -f "$CASSANDRA_HOME/$CASSANDRA_TAR" ]]; then
    echo "📦 Downloading Cassandra $CASSANDRA_VERSION..."
    curl -L "$CASSANDRA_URL" -o "$CASSANDRA_HOME/$CASSANDRA_TAR"
fi

# Extract Cassandra if not already extracted
CASSANDRA_DIR="$CASSANDRA_HOME/apache-cassandra-$CASSANDRA_VERSION"
if [[ ! -d "$CASSANDRA_DIR" ]]; then
    echo "📦 Extracting Cassandra..."
    cd "$CASSANDRA_HOME"
    tar -xzf "$CASSANDRA_TAR"
fi

# Create sstabledump wrapper script
SSTABLEDUMP_SCRIPT="$INSTALL_DIR/sstabledump"
cat > "$SSTABLEDUMP_SCRIPT" << EOF
#!/bin/bash
# sstabledump wrapper for CQLite development
export JAVA_HOME="\$(dirname "\$(dirname "\$(readlink -f "\$(command -v $JAVA_CMD)")")")" 2>/dev/null || export JAVA_HOME=""
export CASSANDRA_HOME="$CASSANDRA_DIR"
exec "$JAVA_CMD" -cp "\$CASSANDRA_HOME/lib/*" org.apache.cassandra.tools.SSTableDump "\$@"
EOF

chmod +x "$SSTABLEDUMP_SCRIPT"

# Verify installation
echo "🔍 Verifying sstabledump installation..."
if "$SSTABLEDUMP_SCRIPT" --help >/dev/null 2>&1; then
    echo "✅ sstabledump installed successfully!"
    echo ""
    echo "📝 Usage:"
    echo "   sstabledump --help"
    echo "   sstabledump /path/to/sstable-Data.db"
    echo ""
    echo "🔧 Add to PATH by adding this to your shell profile:"
    echo "   export PATH=\"$INSTALL_DIR:\$PATH\""
    echo ""
    if [[ ":$PATH:" != *":$INSTALL_DIR:"* ]]; then
        echo "⚠️ $INSTALL_DIR is not in your PATH. Run:"
        echo "   export PATH=\"$INSTALL_DIR:\$PATH\""
    fi
else
    echo "❌ sstabledump installation failed"
    echo "   Try running with debugging:"
    echo "   bash -x $0"
    exit 1
fi

echo ""
echo "🎉 Installation complete! You can now run CQLite parity tests:"
echo "   cargo test --test sstabledump_parity_statistics"