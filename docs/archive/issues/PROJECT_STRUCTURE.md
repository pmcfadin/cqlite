# 🏗️ CQLite Project Structure

## 🚨 MANDATORY READING FOR ALL DEVELOPERS

This document **MUST** be read by ALL team members. The project structure is **ENFORCED** and deviations will be **REJECTED**.

---

## 📁 Directory Structure

### **Root Level (RESTRICTED)**
```
cqlite/
├── Cargo.toml              # ONLY workspace manifest allowed
├── README.md               # Single project README
├── LICENSE                 # Project license
├── .gitignore             # Git ignore rules
└── [ESSENTIAL FILES ONLY]  # Max 30 files in root
```

**🚫 FORBIDDEN IN ROOT:**
- ❌ Individual `.rs` files
- ❌ Additional `Cargo.toml` files
- ❌ Build artifacts (`target/`, `*.log`)
- ❌ Temporary files
- ❌ Database files (`*.db`)

**✅ ALLOWED HIDDEN DEV DIRECTORIES IN ROOT (documented):**
- `.claude/`, `.claude-flow/`, `.swarm/`, `.hive-mind/`, `.roo/`
- `claude-flow*` launcher scripts
- `.mcp.json`, `.roomodes`

These are AI/coordination/dev automation assets and are not part of the Rust workspace. Do not rely on their presence at build time. They remain at root for developer tooling only.

### **Core Crates**

#### **`cqlite-core/`** - Core Library
```
cqlite-core/
├── Cargo.toml              # Crate manifest
├── src/
│   ├── lib.rs             # Library root
│   ├── parser/            # SSTable parsing
│   ├── storage/           # Storage engine
│   ├── query/             # Query execution
│   └── types.rs           # Core types
├── examples/              # Core examples
└── benches/               # Benchmarks
```

#### **`cqlite-cli/`** - CLI Application
```
cqlite-cli/
├── Cargo.toml              # CLI manifest
├── src/
│   ├── main.rs            # CLI entry point
│   ├── commands/          # CLI commands
│   └── config.rs          # Configuration
└── tests/                 # CLI-specific tests
```

#### **`cqlite-ffi/`** - C FFI Bindings
```
cqlite-ffi/
├── Cargo.toml
├── src/lib.rs
└── cbindgen.toml
```

#### **`cqlite-wasm/`** - WASM Bindings
```
cqlite-wasm/
├── Cargo.toml
├── src/lib.rs
└── pkg/                   # Generated WASM output
```

### **Supporting Directories**

#### **`tests/`** - SINGLE Test Directory
```
tests/
├── Cargo.toml              # Test crate manifest
├── src/
│   ├── lib.rs             # Test library
│   ├── integration/       # Integration tests
│   ├── compatibility/     # Compatibility tests
│   └── benchmarks/        # Performance tests
└── data/                  # Test data
```

**🚫 NO OTHER TEST DIRECTORIES ALLOWED**

#### **`examples/`** - Usage Examples
```
examples/
├── Cargo.toml              # Examples manifest
└── proof-of-concept/       # Original PoC code
    ├── src/bin/           # Example binaries
    └── lib.rs             # Example library
```

#### **`tools/`** - Development Tools
```
tools/
├── cqlite-validator/       # SSTable validator
├── format-validator/       # Format checker
└── [APPROVED TOOLS ONLY]   # Must have Cargo.toml
```

#### **`docs/`** - Documentation
```
docs/
├── README.md               # Documentation index
├── technical/              # Technical specs
├── user-guides/           # User documentation
└── development/           # Development guides
```

---

## 🔧 Development Workflow

### **Build Commands**
```bash
# Build entire workspace
cargo build

# Build specific crate
cargo build -p cqlite-core
cargo build -p cqlite-cli

# Run tests
cargo test --workspace

# Run CLI
cargo run -p cqlite-cli -- --help
```

### **Adding New Features**
1. **ALWAYS** add to appropriate crate
2. **NEVER** create loose files in root
3. **NEVER** create additional test directories
4. **UPDATE** this document if structure changes

### **File Organization Rules**

#### **✅ ALLOWED:**
- Add files to existing crate directories
- Create subdirectories within crates
- Add tools with proper `Cargo.toml`

#### **❌ FORBIDDEN:**
- Loose `.rs` files in root
- Multiple `Cargo.toml` in same directory
- Build artifacts in git
- Temporary or cache files
- Database files outside `target/`

---

## 🚨 Quality Gates

### **Pre-Commit Requirements**
1. `cargo check --workspace` MUST pass
2. `cargo clippy --workspace` MUST pass
3. No loose files in root directory
4. All crates must have proper manifests

### **CI/CD Pipeline**
```yaml
- name: Structure Validation
  run: |
    # Ensure no loose files in root
    [ $(find . -maxdepth 1 -type f | wc -l) -le 30 ]
    
    # Ensure workspace compiles
    cargo check --workspace
    
    # Ensure tests compile
    cargo test --workspace --no-run
```

---

## 📋 Team Responsibilities

### **Senior Systems Architect** (THIS ROLE)
- **ENFORCE** project structure
- **REJECT** pull requests violating structure
- **MAINTAIN** this documentation

### **Developers**
- **FOLLOW** structure guidelines
- **READ** this document before contributing
- **ASK** before creating new directories

### **Code Reviewers**
- **VERIFY** structure compliance
- **CHECK** for loose files in root
- **ENSURE** proper crate organization

---

## 🔄 Maintenance

### **Weekly Structure Audit**
```bash
# Run structure validation
./scripts/ci/validate-structure.sh

# Clean up any violations
find . -maxdepth 1 -name "*.rs" -delete
find . -name "target" -type d -exec rm -rf {} +
```

### **Monthly Cleanup**
- Review and remove unused files
- Consolidate duplicate functionality
- Update documentation

---

## 🚫 ZERO TOLERANCE POLICY

**The following violations will result in IMMEDIATE PR REJECTION:**

1. **Loose files in root directory**
2. **Multiple Cargo.toml in same location**
3. **Build artifacts in git**
4. **Duplicate test directories**
5. **Unorganized crate structure**

---

## 📞 Escalation

If you encounter structure issues or need clarification:

1. **Check this documentation first**
2. **Review existing crate patterns**
3. **Escalate to Senior Systems Architect**

**Remember: This structure is NOT a suggestion - it's MANDATORY.**

---

*Last Updated: 2025-07-31 by Senior Systems Architect*
*Next Review: Weekly*