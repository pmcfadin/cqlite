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
    checkout_test_data_dir, readable_file, resolve_datasets_root, resolve_datasets_root_if_present,
    resolve_schema_path, resolve_schemas_root, schema_path, schemas_root, workspace_root_from,
    SchemasRootSource,
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
    // A GUARANTEED-absent absolute path, built under a fresh TempDir with native path
    // handling rather than hard-coded as `/nonexistent-…` (roborev job 10, finding 3): a
    // hard-coded root-level name is not absolute under Windows path semantics and is not
    // guaranteed absent on Unix — someone can create it, and then this test would silently
    // assert the OPPOSITE branch. The TempDir is unique per run and the child is never
    // created, so absence is a property of the construction.
    let tmp = tempfile::TempDir::new().expect("temp dir");
    let absent = tmp.path().join("no-such-schemas-dir");
    assert!(absent.is_absolute(), "TempDir children are absolute");
    assert!(!absent.exists(), "the child is deliberately never created");
    let raw = absent.to_str().expect("utf-8 temp path");

    let (root, source) = resolve_schemas_root(Some(raw))
        .expect("an absolute-but-absent override degrades, it is not an error");
    assert_eq!(source, SchemasRootSource::Checkout);
    assert_eq!(root, checkout_test_data_dir().join("schemas"));
}

/// A control-character-bearing override is REJECTED on both sides (roborev job 10, finding
/// 2). The gate's shell mirror can only obtain the value through command substitution
/// somewhere in its history, and `$( )` strips trailing newlines — so admitting such a value
/// let the gate certify `/abs/dir` while this resolver kept the newline, failed `is_dir()`,
/// and degraded to the checkout. Rejecting closes the divergence for good.
#[test]
fn control_character_override_is_rejected_fail_closed() {
    let tmp = tempfile::TempDir::new().expect("temp dir");
    let dir = tmp.path().to_str().expect("utf-8 temp path");

    for (label, raw) in [
        ("trailing newline", format!("{dir}\n")),
        ("leading newline", format!("\n{dir}")),
        ("carriage return", format!("{dir}\r")),
        ("embedded tab", format!("{dir}\tsub")),
    ] {
        match resolve_schemas_root(Some(&raw)) {
            Err(e) => {
                assert!(
                    e.contains("control characters"),
                    "{label}: rejection must name the rule; got: {e}"
                );
                assert!(
                    e.contains("remedy"),
                    "{label}: rejection must be actionable; got: {e}"
                );
            }
            Ok((root, source)) => panic!(
                "{label}: a control-character override was ACCEPTED ({} via {source:?}) — the \
                 gate's shell mirror strips trailing newlines, so this is a root-certification \
                 divergence, not a cosmetic issue",
                root.display()
            ),
        }
    }
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

/// #3148 requirement 5: the unreadable-fixture message is the DELIVERABLE, so it is asserted.
///
/// Before this it was covered by nothing — the only test was the happy path, so reverting
/// `schema_path` to a bare `expect` deep inside ingest left every test green. That is the
/// diagnosis-free failure this issue was filed for, so the message gets a real assertion:
/// the absolute path, the root, HOW the root was chosen, the committed-source note, and the
/// remedy must all be present.
#[test]
fn unreadable_fixture_message_names_path_root_source_and_remedy() {
    let tmp = tempfile::TempDir::new().expect("temp dir");
    let root = tmp.path();

    let err = resolve_schema_path(root, SchemasRootSource::Checkout, "basic-types.cql")
        .expect_err("an absent fixture must not resolve");
    let expected_path = root.join("basic-types.cql");
    assert!(
        err.contains(&expected_path.display().to_string()),
        "message must name the ABSOLUTE fixture path; got: {err}"
    );
    assert!(
        err.contains(&root.display().to_string()),
        "message must name the resolved root; got: {err}"
    );
    assert!(
        err.contains("checkout-relative"),
        "message must say HOW the root was chosen; got: {err}"
    );
    assert!(
        err.contains("COMMITTED SOURCE") && err.contains("CQLITE_DATASETS_ROOT"),
        "message must state these are committed source, not fetched data; got: {err}"
    );
    assert!(
        err.contains("remedy") && err.contains("restore --source=HEAD -- test-data/schemas"),
        "message must carry the remedy command; got: {err}"
    );
    // The override source must be distinguishable in the SAME message slot.
    let err_override = resolve_schema_path(root, SchemasRootSource::EnvOverride, "x.cql")
        .expect_err("an absent fixture must not resolve");
    assert!(
        err_override.contains("CQLITE_SCHEMAS_ROOT override"),
        "an override root must be reported as such; got: {err_override}"
    );
}

/// …and `schema_path` itself must PANIC with that message, not merely be able to build it.
/// Uses `catch_unwind` over a fixture name that cannot exist in the real checkout, so no
/// environment mutation is needed.
#[test]
fn schema_path_panics_with_the_actionable_message() {
    let missing = "definitely-not-a-committed-schema-3148.cql";
    // Silence the default hook so an EXPECTED panic does not print a scary backtrace.
    let prev = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let outcome = std::panic::catch_unwind(|| schema_path(missing));
    std::panic::set_hook(prev);

    let payload = outcome.expect_err("schema_path must panic on an unreadable fixture");
    let msg = payload
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| payload.downcast_ref::<&str>().copied())
        .unwrap_or("<non-string panic payload>");
    let expected_path = schemas_root().join(missing);
    assert!(
        msg.contains(&expected_path.display().to_string()),
        "the panic must name the absolute path (not a bare 'Path does not exist'); got: {msg}"
    );
    assert!(
        msg.contains("remedy"),
        "the panic must be actionable; got: {msg}"
    );
}

/// #3148 AC (e) / requirement 6: the two `datasets_root` shapes DIFFER, and the difference is
/// the contract. Asserted BEHAVIOURALLY — the previous guard grepped for the function name, so
/// giving the fallible shape a checkout fallback (collapsing the two into one) reddened nothing.
#[test]
fn the_two_datasets_root_shapes_differ_as_documented() {
    let checkout_default = checkout_test_data_dir().join("datasets");

    // Infallible shape: falls back to the checkout when there is no value…
    assert_eq!(resolve_datasets_root(None), checkout_default);
    for blank in ["", "   "] {
        assert_eq!(
            resolve_datasets_root(Some(blank)),
            checkout_default,
            "an exported-but-blank value is a scripting accident, never a root"
        );
    }
    // …and returns a value it is given even when that value is not a directory (the per-fixture
    // error later is more actionable than a root-level one — that is the documented choice).
    let tmp = tempfile::TempDir::new().expect("temp dir");
    let absent = tmp.path().join("no-such-corpus");
    let absent_raw = absent.to_str().expect("utf-8");
    assert_eq!(resolve_datasets_root(Some(absent_raw)), absent);

    // Fallible shape: NO checkout fallback — this is the assertion that catches a collapse.
    assert_eq!(
        resolve_datasets_root_if_present(None),
        None,
        "the fallible shape must NOT fall back to the checkout: a skip-gated test would then \
         run against committed byte-parity references and report a vacuous 0-row pass"
    );
    for blank in ["", "   "] {
        assert_eq!(resolve_datasets_root_if_present(Some(blank)), None);
    }
    assert_eq!(
        resolve_datasets_root_if_present(Some(absent_raw)),
        None,
        "a value that is not a directory yields None"
    );
    let present = tmp.path().to_str().expect("utf-8");
    assert_eq!(
        resolve_datasets_root_if_present(Some(present)),
        Some(tmp.path().to_path_buf()),
        "a value naming a real directory is returned"
    );
}
