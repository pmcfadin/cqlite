# Building and Publishing

## Development Workflow

```bash
# Install dependencies
npm install

# Build debug (faster, for development)
npm run build:debug

# Build release (optimized)
npm run build

# Run tests
npm test
```

## Project Configuration

### package.json

```json
{
  "name": "@yourorg/cqlite",
  "version": "0.1.0",
  "main": "index.js",
  "types": "index.d.ts",
  "napi": {
    "name": "cqlite",
    "triples": {
      "defaults": true,
      "additional": [
        "aarch64-apple-darwin",
        "aarch64-unknown-linux-gnu",
        "aarch64-unknown-linux-musl",
        "aarch64-pc-windows-msvc",
        "armv7-unknown-linux-gnueabihf",
        "x86_64-unknown-linux-musl",
        "x86_64-unknown-freebsd",
        "i686-pc-windows-msvc"
      ]
    }
  },
  "license": "MIT",
  "engines": {
    "node": ">= 14"
  },
  "scripts": {
    "artifacts": "napi artifacts",
    "build": "napi build --platform --release",
    "build:debug": "napi build --platform",
    "prepublishOnly": "napi prepublish -t npm",
    "test": "ava",
    "version": "napi version"
  },
  "devDependencies": {
    "@napi-rs/cli": "^2.18.0",
    "ava": "^6.0.0"
  },
  "files": [
    "index.js",
    "index.d.ts"
  ],
  "publishConfig": {
    "access": "public"
  }
}
```

### Cargo.toml

```toml
[package]
name = "cqlite"
version = "0.1.0"
edition = "2021"

[lib]
crate-type = ["cdylib"]

[dependencies]
napi = { version = "2", default-features = false, features = [
    "napi9",
    "async",
    "serde-json",
    "tokio_rt"
] }
napi-derive = "2"
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"

[build-dependencies]
napi-build = "2"

[profile.release]
lto = true
opt-level = 3
codegen-units = 1
strip = true
```

### build.rs

```rust
extern crate napi_build;

fn main() {
    napi_build::setup();
}
```

## Generated Files

After build, napi-rs generates:

```
├── index.js           # JS bindings (auto-generated)
├── index.d.ts         # TypeScript definitions (auto-generated)
├── cqlite.darwin-arm64.node    # macOS ARM binary
├── cqlite.darwin-x64.node      # macOS Intel binary
├── cqlite.win32-x64-msvc.node  # Windows binary
├── cqlite.linux-x64-gnu.node   # Linux binary
└── ...
```

## Publishing to npm

### 1. Test Locally First

```bash
# Build release
npm run build

# Test the build
npm test

# Pack to verify contents
npm pack --dry-run
```

### 2. Publish with Platform Packages

napi-rs publishes platform-specific packages automatically:

```bash
# Prepare for publishing (creates platform packages)
npm run prepublishOnly

# Publish all packages
npm publish
```

### 3. CI/CD with GitHub Actions

```yaml
# .github/workflows/ci.yml
name: CI
on:
  push:
    branches: [main]
  pull_request:

jobs:
  build:
    strategy:
      fail-fast: false
      matrix:
        settings:
          - host: macos-latest
            target: x86_64-apple-darwin
            build: npm run build
          - host: macos-latest
            target: aarch64-apple-darwin
            build: npm run build -- --target aarch64-apple-darwin
          - host: ubuntu-latest
            target: x86_64-unknown-linux-gnu
            build: npm run build
          - host: ubuntu-latest
            target: x86_64-unknown-linux-musl
            build: npm run build -- --target x86_64-unknown-linux-musl
          - host: windows-latest
            target: x86_64-pc-windows-msvc
            build: npm run build
          - host: ubuntu-latest
            target: aarch64-unknown-linux-gnu
            build: npm run build -- --target aarch64-unknown-linux-gnu
    runs-on: ${{ matrix.settings.host }}
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with:
          node-version: 20
      - uses: dtolnay/rust-toolchain@stable
        with:
          targets: ${{ matrix.settings.target }}
      - run: npm ci
      - run: ${{ matrix.settings.build }}
      - uses: actions/upload-artifact@v4
        with:
          name: bindings-${{ matrix.settings.target }}
          path: "*.node"

  test:
    needs: build
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with:
          node-version: 20
      - uses: actions/download-artifact@v4
        with:
          name: bindings-x86_64-unknown-linux-gnu
      - run: npm ci
      - run: npm test

  publish:
    if: github.event_name == 'push' && startsWith(github.ref, 'refs/tags/v')
    needs: [build, test]
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with:
          node-version: 20
          registry-url: https://registry.npmjs.org
      - uses: actions/download-artifact@v4
        with:
          path: artifacts
      - run: npm ci
      - run: napi artifacts --dir artifacts
      - run: npm run prepublishOnly
      - run: npm publish
        env:
          NODE_AUTH_TOKEN: ${{ secrets.NPM_TOKEN }}
```

## Platform-Specific Packages

napi-rs creates optional dependencies for each platform:

```json
{
  "name": "@yourorg/cqlite",
  "optionalDependencies": {
    "@yourorg/cqlite-darwin-arm64": "0.1.0",
    "@yourorg/cqlite-darwin-x64": "0.1.0",
    "@yourorg/cqlite-linux-x64-gnu": "0.1.0",
    "@yourorg/cqlite-win32-x64-msvc": "0.1.0"
  }
}
```

Users install the main package, and npm automatically installs the correct platform binary.

## Version Management

```bash
# Bump version in both package.json and Cargo.toml
napi version patch  # 0.1.0 -> 0.1.1
napi version minor  # 0.1.0 -> 0.2.0
napi version major  # 0.1.0 -> 1.0.0
```

## Hybrid Package (Rust + JavaScript)

```
cqlite/
├── Cargo.toml
├── package.json
├── src/
│   └── lib.rs          # Rust code
├── lib/
│   ├── index.js        # Pure JS additions
│   └── helpers.js
└── index.js            # Entry point
```

```javascript
// index.js - Main entry point
const { existsSync, readFileSync } = require('fs');
const { join } = require('path');

// Load native addon
const { parse, Statement } = require('./cqlite.node');

// Add pure JS utilities
const { formatStatement } = require('./lib/helpers');

module.exports = {
  parse,
  Statement,
  formatStatement,
};
```

## Pre-built Binary Distribution

For faster installs without compilation:

```json
{
  "scripts": {
    "postinstall": "node scripts/download-binary.js"
  }
}
```

Or use the napi-rs default approach with `optionalDependencies` which handles this automatically.
