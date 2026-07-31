//! Contract tests for the shared fixture-root resolver (issues #3148 / #3131).
//!
//! These pin the resolution RULES that `scripts/agent-gate.sh` mirrors. The gate stamps a
//! `schemas: N/N … under <root>` line into the SUMMARY on the strength of its own shell
//! copy of these rules, so any divergence means the gate certifies a root the tests never
//! used — the "positively misleading `STATUS: OK`" defect #3148 was filed for. The shell
//! half is pinned by `scripts/tests/test_agent_gate_schemas_preflight.sh`; this is the
//! Rust half, and the two assert the SAME table.
//!
//! Deliberately env-free: every case drives the PURE
//! [`fixture_roots::resolve_schemas_root`] with an explicit override value instead of
//! mutating `CQLITE_SCHEMAS_ROOT`. Env vars are process-global, so an env-mutating test
//! races every other test in the binary — and a flaky guard gets deleted, not fixed.

use std::path::{Path, PathBuf};

#[path = "../../test-data/support/fixture_roots.rs"]
mod fixture_roots;

use fixture_roots::{
    checkout_test_data_dir, readable_file, resolve_schemas_root, schema_path, schemas_root,
    workspace_root_from, SchemasRootSource,
};

/// A RELATIVE override is REJECTED, not resolved.
///
/// This is the case that nearly reintroduced #3148's own defect: the gate evaluates a
/// relative path with CWD = repository root, cargo runs each test binary with CWD = the
/// PACKAGE directory. Resolving it would let the gate stamp
/// `schemas: 6/6 … under packaged/schemas (override)` while every test binary silently
/// fell back to the checkout and read DIFFERENT files.
#[test]
fn relative_schemas_override_is_rejected_fail_closed() {
    let err = resolve_schemas_root(Some("packaged/schemas"))
        .expect_err("a relative CQLITE_SCHEMAS_ROOT must be rejected, not resolved");
    assert!(
        err.contains("must be an ABSOLUTE path"),
        "rejection must name the rule; got: {err}"
    );
    assert!(
        err.contains("packaged/schemas"),
        "rejection must quote the offending value; got: {err}"
    );
    // Actionability: the message must explain the CWD asymmetry AND give a remedy,
    // otherwise an operator reads it as an arbitrary restriction and works around it.
    assert!(
        err.contains("CWD") && err.contains("remedy"),
        "rejection must explain why and how to fix; got: {err}"
    );
    // Every relative shape, not just the bare one.
    for raw in ["./schemas", "../schemas", "a/b/schemas"] {
        assert!(
            resolve_schemas_root(Some(raw)).is_err(),
            "relative override {raw:?} must be rejected"
        );
    }
}

/// Absent or blank means "no override" — an exported-but-empty variable is a scripting
/// accident, never an intentional root. Mirrors the gate's whitespace-stripped check.
#[test]
fn absent_or_blank_override_resolves_to_the_checkout() {
    for raw in [None, Some(""), Some("   "), Some("\t")] {
        let (root, source) = resolve_schemas_root(raw).expect("blank override is not an error");
        assert_eq!(
            source,
            SchemasRootSource::Checkout,
            "raw={raw:?} must resolve checkout-relative"
        );
        assert_eq!(root, checkout_test_data_dir().join("schemas"));
    }
}

/// An ABSOLUTE override that is not a usable directory DEGRADES to the checkout rather
/// than pinning the run to a path that cannot work — a stale export in a shell profile
/// must not break every fixture load. (A relative one is rejected instead; see above.)
#[test]
fn absolute_but_unusable_override_degrades_to_the_checkout() {
    let (root, source) = resolve_schemas_root(Some("/nonexistent-cqlite-schemas-3148"))
        .expect("an absolute-but-absent override degrades, it is not an error");
    assert_eq!(source, SchemasRootSource::Checkout);
    assert_eq!(root, checkout_test_data_dir().join("schemas"));
}

/// An ABSOLUTE override naming a real directory wins, and is reported as an override so
/// a failure message can distinguish it from the checkout default.
#[test]
fn absolute_existing_override_wins_and_is_reported_as_such() {
    let tmp = tempfile::TempDir::new().expect("temp dir");
    let raw = tmp.path().to_str().expect("utf-8 temp path");
    let (root, source) = resolve_schemas_root(Some(raw)).expect("absolute readable dir is valid");
    assert_eq!(source, SchemasRootSource::EnvOverride);
    assert_eq!(root, PathBuf::from(raw));
}

/// The checkout is identified by a checkout MARKER (the workspace-root `Cargo.toml`), not
/// by the fixtures. Keying the ancestor walk on `test-data/schemas` let a sparse checkout
/// — or a worktree nested inside another checkout — resolve to the OUTER checkout's
/// fixtures: wrong-but-existing, reported as `Checkout`, no warning.
///
/// Asserted structurally: the resolved `test-data` must be a DIRECT child of an ancestor
/// that carries a `[workspace]` manifest, and that ancestor must be THIS crate's own
/// workspace root (the parent of `CARGO_MANIFEST_DIR`), never some outer one.
#[test]
fn checkout_test_data_dir_is_anchored_on_the_workspace_marker() {
    let td = checkout_test_data_dir();
    let root = td.parent().expect("test-data has a parent");
    let manifest = root.join("Cargo.toml");
    let text = std::fs::read_to_string(&manifest)
        .unwrap_or_else(|e| panic!("workspace manifest {} unreadable: {e}", manifest.display()));
    assert!(
        text.lines()
            .any(|l| l.trim_start().starts_with("[workspace]")),
        "{} must be the WORKSPACE root manifest",
        manifest.display()
    );
    // …and it must be OUR workspace root: the parent of this crate's manifest dir.
    let expected_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crate dir has a parent");
    assert_eq!(
        root, expected_root,
        "resolution escaped this checkout (nested-worktree hazard)"
    );
    assert!(td.is_dir(), "{} must exist in a checkout", td.display());
}

/// The DISCRIMINATING control for the marker rule: a worktree nested inside another
/// checkout must resolve to ITS OWN root, not the outer one.
///
/// The test above cannot catch a revert of that rule — in a healthy checkout the retired
/// fixtures-keyed walk returns the same path, so it would still pass. This one drives
/// `workspace_root_from` over a synthetic layout where the two rules DISAGREE:
///
/// ```text
/// outer/Cargo.toml            [workspace]
/// outer/test-data/schemas/    <- the fixtures the retired walk would have latched onto
/// outer/inner/Cargo.toml      [workspace]   <- the nested worktree's OWN root
/// outer/inner/cqlite-core/Cargo.toml        (a member manifest, no [workspace])
/// ```
///
/// Marker walk from `outer/inner/cqlite-core` ⇒ `outer/inner` (correct: this checkout).
/// Fixtures-keyed walk ⇒ `outer` (wrong: the neighbour's tree, silently, and it EXISTS —
/// which is exactly why the bug was invisible).
#[test]
fn nested_worktree_resolves_to_its_own_root_not_the_outer_checkout() {
    let tmp = tempfile::TempDir::new().expect("temp dir");
    let outer = tmp.path().join("outer");
    let inner = outer.join("inner");
    let member = inner.join("cqlite-core");
    std::fs::create_dir_all(outer.join("test-data").join("schemas")).expect("outer fixtures");
    std::fs::create_dir_all(&member).expect("member dir");
    std::fs::write(outer.join("Cargo.toml"), "[workspace]\nmembers = []\n")
        .expect("outer manifest");
    std::fs::write(
        inner.join("Cargo.toml"),
        "[workspace]\nmembers = [\"cqlite-core\"]\n",
    )
    .expect("inner manifest");
    // A MEMBER manifest: no `[workspace]`, so the walk must pass through it.
    std::fs::write(
        member.join("Cargo.toml"),
        "[package]\nname = \"cqlite-core\"\n",
    )
    .expect("member manifest");

    let resolved = workspace_root_from(&member).expect("a [workspace] ancestor exists");
    assert_eq!(
        resolved, inner,
        "resolution escaped into the OUTER checkout — the marker rule regressed to a \
         fixtures-keyed walk, which silently borrows a neighbour's test-data"
    );
    // Positive control on the layout itself: the outer fixtures really are present, so the
    // assertion above discriminates the two rules instead of passing on an empty tree.
    assert!(
        outer.join("test-data").join("schemas").is_dir(),
        "the discriminating layout requires the OUTER fixtures to exist"
    );
    assert!(
        !inner.join("test-data").join("schemas").exists(),
        "the discriminating layout requires the INNER fixtures to be ABSENT"
    );
}

/// `readable_file` answers "readable REGULAR file", which is the same question the gate's
/// `[ -f ] && [ -r ]` asks. `Path::is_file()` alone would accept a mode-000 fixture (which
/// then fails inside ingestion, bypassing the actionable message), and a bare `-r` would
/// accept a DIRECTORY named `basic-types.cql`.
#[test]
fn readable_file_rejects_directories_and_unreadable_files() {
    let tmp = tempfile::TempDir::new().expect("temp dir");

    let dir_named_like_a_schema = tmp.path().join("basic-types.cql");
    std::fs::create_dir(&dir_named_like_a_schema).expect("create dir");
    assert!(
        !readable_file(&dir_named_like_a_schema),
        "a directory named like a schema file is not a readable regular file"
    );

    let real = tmp.path().join("real.cql");
    std::fs::write(&real, b"CREATE TABLE x (k int PRIMARY KEY);\n").expect("write");
    assert!(readable_file(&real), "a real readable file must pass");

    // Permission case, guarded: a run as root (some CI containers) bypasses mode bits, so
    // only assert it where the OS actually enforces them — proven by a control probe
    // rather than assumed. Skipping loudly beats a host-conditional flake.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let locked = tmp.path().join("locked.cql");
        std::fs::write(&locked, b"x").expect("write");
        std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o000))
            .expect("chmod 000");
        if std::fs::File::open(&locked).is_err() {
            assert!(
                !readable_file(&locked),
                "an unreadable regular file must not pass"
            );
        } else {
            eprintln!("note: permissions not enforced for this user; skipping the mode-000 case");
        }
    }
}

/// End-to-end in a real checkout: every schema file the gate's preflight asserts must be
/// resolvable through the same public helper the migrated call sites use. This is the
/// cross-check that makes the gate's `CANONICAL_SCHEMA_FILES` list meaningful — if the
/// list and the resolver disagreed, the gate would pass while the tests panicked.
#[test]
fn every_gate_asserted_schema_resolves_in_a_checkout() {
    const GATE_ASSERTED: [&str; 6] = [
        "basic-types.cql",
        "da-test.cql",
        "time-series.cql",
        "wide-table-bti.cql",
        "collections.cql",
        "wide-rows.cql",
    ];
    let root = schemas_root();
    for f in GATE_ASSERTED {
        let p = schema_path(f); // panics with the actionable message if unreadable
        assert_eq!(p, root.join(f));
        assert!(readable_file(&p), "{} must be readable", p.display());
    }
}
