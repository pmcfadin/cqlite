//! The no-FFI-framework rule of `cqlite-ffi-common`, **measured** (issue #1452).
//!
//! This crate exists so both language bindings can share one implementation of
//! the scalar byte-math and the error contract. That is only possible while the
//! crate links **neither** binding's FFI framework: a `pyo3` dependency would
//! make it unusable from Node, a `napi` dependency unusable from Python.
//!
//! # Why a manifest grep is not enough
//!
//! Reading `Cargo.toml` answers only about *direct* dependencies, and a positive
//! verdict derived from the absence of a bad signal is exactly the shape CLAUDE.md
//! forbids. So this test takes an **affirmative transitive measurement**: it asks
//! `cargo metadata` for the resolved dependency graph, walks the closure rooted at
//! the `cqlite-ffi-common` package, and inspects every package name it actually
//! resolves to.
//!
//! # Fail-closed in every direction
//!
//! `CARGO` unset, a non-zero `cargo metadata` exit, output that does not parse,
//! a resolve section that is absent, this package's own node not being found, an
//! empty closure, or a closure missing `cqlite-core` each **FAIL** with a message
//! naming the measurement that could not be taken. There is no
//! "could not check, assume fine" branch and no environment variable that skips
//! or softens any part of it.

use std::collections::{BTreeSet, HashSet, VecDeque};
use std::process::Command;

/// Package names that must never appear in this crate's resolved closure.
///
/// Matched as an exact name OR as a name prefix, so `napi-derive`, `napi-sys`,
/// `pyo3-ffi`, `pyo3-macros`, … are all caught by the three roots.
const FORBIDDEN_PREFIXES: &[&str] = &["pyo3", "napi", "napi-derive"];

/// This crate's package name — the root of the closure being measured.
const SELF_PACKAGE: &str = "cqlite-ffi-common";

/// A package the closure MUST contain. Its absence means the walk resolved
/// something other than the real graph, so a clean verdict would be vacuous.
const REQUIRED_PACKAGE: &str = "cqlite-core";

/// Run `cargo metadata` and return its stdout, or an error naming what could not
/// be measured.
fn cargo_metadata_json() -> Result<String, String> {
    // `CARGO` is set by cargo for every test process. Its absence means we are
    // not running under the toolchain we intend to measure, which is a failure
    // to measure — never a pass.
    let cargo = std::env::var("CARGO").map_err(|e| {
        format!(
            "cannot measure the dependency closure: the CARGO environment \
             variable is unset or not valid UTF-8 ({e}); `cargo metadata` could \
             not be located"
        )
    })?;

    let manifest = format!("{}/Cargo.toml", env!("CARGO_MANIFEST_DIR"));

    let output = Command::new(&cargo)
        .args([
            "metadata",
            "--format-version",
            "1",
            "--locked",
            "--all-features",
            "--manifest-path",
            &manifest,
        ])
        .output()
        .map_err(|e| {
            format!(
                "cannot measure the dependency closure: failed to execute \
                 `{cargo} metadata` ({e})"
            )
        })?;

    if !output.status.success() {
        return Err(format!(
            "cannot measure the dependency closure: `{cargo} metadata` exited \
             with {status}; stderr:\n{stderr}",
            status = output.status,
            stderr = String::from_utf8_lossy(&output.stderr),
        ));
    }

    String::from_utf8(output.stdout).map_err(|e| {
        format!("cannot measure the dependency closure: `cargo metadata` stdout is not UTF-8 ({e})")
    })
}

/// The set of package names reachable from `cqlite-ffi-common` in the resolve
/// graph, or an error naming what could not be measured.
fn resolved_closure(json: &str) -> Result<BTreeSet<String>, String> {
    let meta: serde_json::Value = serde_json::from_str(json).map_err(|e| {
        format!(
            "cannot measure the dependency closure: `cargo metadata` output did not parse ({e})"
        )
    })?;

    // package id -> name, for turning resolve ids back into names.
    let packages = meta
        .get("packages")
        .and_then(|p| p.as_array())
        .ok_or_else(|| {
            "cannot measure the dependency closure: `cargo metadata` output has no \
             `packages` array"
                .to_string()
        })?;
    let mut name_of: std::collections::HashMap<&str, &str> = std::collections::HashMap::new();
    for pkg in packages {
        let id = pkg.get("id").and_then(|v| v.as_str()).ok_or_else(|| {
            "cannot measure the dependency closure: a `packages` entry has no string `id`"
                .to_string()
        })?;
        let name = pkg.get("name").and_then(|v| v.as_str()).ok_or_else(|| {
            format!("cannot measure the dependency closure: package `{id}` has no string `name`")
        })?;
        name_of.insert(id, name);
    }

    let nodes = meta
        .get("resolve")
        .and_then(|r| r.get("nodes"))
        .and_then(|n| n.as_array())
        .ok_or_else(|| {
            "cannot measure the dependency closure: `cargo metadata` output has no \
             `resolve.nodes` (was it run with --no-deps?)"
                .to_string()
        })?;
    let mut deps_of: std::collections::HashMap<&str, Vec<&str>> = std::collections::HashMap::new();
    for node in nodes {
        let id = node.get("id").and_then(|v| v.as_str()).ok_or_else(|| {
            "cannot measure the dependency closure: a `resolve.nodes` entry has no string `id`"
                .to_string()
        })?;
        let deps = node
            .get("deps")
            .and_then(|d| d.as_array())
            .ok_or_else(|| {
                format!(
                    "cannot measure the dependency closure: resolve node `{id}` has no \
                     `deps` array"
                )
            })?
            .iter()
            .map(|d| {
                d.get("pkg").and_then(|v| v.as_str()).ok_or_else(|| {
                    format!(
                        "cannot measure the dependency closure: a dep of resolve node \
                         `{id}` has no string `pkg`"
                    )
                })
            })
            .collect::<Result<Vec<&str>, String>>()?;
        deps_of.insert(id, deps);
    }

    // Locate our own node. Not finding it means we measured the wrong graph.
    let self_id = name_of
        .iter()
        .find(|(id, name)| **name == SELF_PACKAGE && deps_of.contains_key(**id))
        .map(|(id, _)| *id)
        .ok_or_else(|| {
            format!(
                "cannot measure the dependency closure: no `resolve.nodes` entry for \
                 package `{SELF_PACKAGE}` — the graph walked is not this crate's"
            )
        })?;

    // Breadth-first walk of the resolve graph from our node.
    let mut seen: HashSet<&str> = HashSet::new();
    let mut queue: VecDeque<&str> = VecDeque::new();
    let mut names = BTreeSet::new();
    queue.push_back(self_id);
    seen.insert(self_id);
    while let Some(id) = queue.pop_front() {
        let name = name_of.get(id).ok_or_else(|| {
            format!(
                "cannot measure the dependency closure: resolve node `{id}` has no \
                 matching `packages` entry"
            )
        })?;
        if *name != SELF_PACKAGE {
            names.insert((*name).to_string());
        }
        let deps = deps_of.get(id).ok_or_else(|| {
            format!(
                "cannot measure the dependency closure: package `{id}` appears as a \
                 dependency but has no resolve node"
            )
        })?;
        for dep in deps {
            if seen.insert(dep) {
                queue.push_back(dep);
            }
        }
    }

    Ok(names)
}

/// `true` when `name` is, or begins with, one of the forbidden FFI roots.
fn is_forbidden(name: &str) -> bool {
    FORBIDDEN_PREFIXES
        .iter()
        .any(|forbidden| name == *forbidden || name.starts_with(forbidden))
}

#[test]
fn resolved_closure_is_free_of_ffi_frameworks() {
    let json = match cargo_metadata_json() {
        Ok(json) => json,
        Err(why) => panic!("{why}"),
    };
    let closure = match resolved_closure(&json) {
        Ok(closure) => closure,
        Err(why) => panic!("{why}"),
    };

    // A resolve that silently returned nothing must not pass vacuously.
    assert!(
        !closure.is_empty(),
        "cannot measure the dependency closure: the closure rooted at \
         `{SELF_PACKAGE}` resolved to zero packages"
    );
    assert!(
        closure.contains(REQUIRED_PACKAGE),
        "cannot measure the dependency closure: `{REQUIRED_PACKAGE}` is not in the \
         closure rooted at `{SELF_PACKAGE}` — the measurement did not reach this \
         crate's real dependencies (resolved: {closure:?})"
    );

    let offenders: Vec<&String> = closure.iter().filter(|n| is_forbidden(n)).collect();
    assert!(
        offenders.is_empty(),
        "`{SELF_PACKAGE}` must not depend on an FFI framework at any depth \
         (issue #1452), but its resolved dependency closure contains: {offenders:?}. \
         A crate that links one binding's framework cannot be shared with the other."
    );
}

/// The forbidden-name predicate itself, so a future rename cannot quietly turn
/// the guard above into a no-op. This is the RED half of the guard held as an
/// assertion: names that MUST be rejected, and names that must NOT be.
#[test]
fn forbidden_predicate_matches_exact_names_and_prefixes() {
    for rejected in [
        "pyo3",
        "pyo3-ffi",
        "pyo3-macros",
        "pyo3-macros-backend",
        "pyo3-build-config",
        "napi",
        "napi-sys",
        "napi-build",
        "napi-derive",
        "napi-derive-backend",
    ] {
        assert!(
            is_forbidden(rejected),
            "`{rejected}` must be recognised as an FFI framework package"
        );
    }
    for allowed in ["cqlite-core", "num-bigint", "serde_json", "num-traits"] {
        assert!(
            !is_forbidden(allowed),
            "`{allowed}` must not be flagged as an FFI framework package"
        );
    }
}
