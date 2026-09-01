#!/usr/bin/env bash
# check-features-load-bearing.sh — the FEATURES-ARE-LOAD-BEARING guard
# (issue #1698, epic #1685 "config honesty").
#
# # What this exists for
#
# Seven cargo features were declared across this workspace's manifests with ZERO
# effect of any kind: `events`, `ci_zero_tolerance` (in five manifests), four
# `test-*` leaves of an aggregator, a `sstable-writer` placeholder, a CLI
# `interactive` that even sat in `default`, and a `cqlite-core/unit-tests-only`
# forwarded from another member. Enabling any of them changed NOTHING that is
# compiled, linked or run. They read — to a human, to a reviewer, to the gate's own
# clippy feature enumerations — as switches that select behaviour, and they select
# nothing. A flag that cannot change the build is a lie about the build.
#
# So this guard answers exactly ONE question, on every full gate:
#
#   Is EVERY feature declared by a workspace member LOAD-BEARING — i.e. does
#   enabling it change something that is compiled, linked, or selected?
#
# # THE PREDICATE
#
# A declared feature F of workspace member P is LOAD-BEARING iff some feature G in
# the FEATURE CLOSURE of F (F itself, plus everything F enables, transitively,
# following `pkg/feature` edges into other workspace members) has a DIRECT EFFECT:
#
#   E1  REFERENCE SITE — G's name is named as a feature INSIDE A REAL CFG PREDICATE
#       (a `cfg`, `cfg_attr` or `cfg!` token followed by a balanced parenthesised
#       predicate — `#[cfg]`/`#![cfg]` differ only in what precedes the token, and
#       `all()`/`any()`/`not()` nesting is inside the same span), or as an actual
#       `CARGO_FEATURE_G` ENV READ in the package's BUILD SCRIPT, in the sources of
#       the package that declares G. A bare textual `feature = "G"` is NOT a site:
#       it occurs in ordinary string literals, in `//!` doc text (this workspace has
#       a live instance — `arbitrary_precision` in cqlite-ffi-common/src/
#       json_number.rs appears only in doc comments), in error messages and in
#       `--features` argument strings, none of which gate anything. Crediting those
#       credits a DEAD feature, which is the false-PASS direction. For
#       `cfg_attr(pred, attrs...)` only the FIRST top-level argument is the
#       predicate: a feature named in the tail
#       (`#[cfg_attr(docsrs, doc(cfg(feature = "x")))]`) gates nothing by itself.
#   E2  OPTIONAL DEPENDENCY — G's dep list enables an optional dependency (`dep:x`).
#       The "bare optional-dep name" spelling (`wasm = ["wasm-bindgen", ...]`) is
#       covered by the closure, because cargo SYNTHESISES an implicit feature per
#       optional dep whose own dep list is exactly `["dep:x"]`. A non-weak
#       `x/feature` on an optional dep also ACTIVATES it, per cargo's documented
#       behaviour, so it counts here too.
#   E2b DEPENDENCY FEATURE — G's dep list enables a feature of a NON-member
#       dependency. Enabling a feature of an external crate demonstrably changes that
#       crate's compiled code, so it is load-bearing by definition. This guard cannot
#       audit non-workspace sources, and does not need to: the effect is established
#       by the edge itself. BUT A WEAK EDGE (`x?/feature`) IS NOT AN EFFECT ON ITS
#       OWN — cargo does nothing with it unless the optional dependency `x` is
#       activated by something else — so a weak edge is credited only when `x` is
#       activated somewhere in the ORIGIN's closure (see below). `observability-
#       testing = ["observability", "opentelemetry_sdk?/testing"]` is live because
#       `observability` activates that dep; a standalone `["x?/f"]` is dead.
#   E3  REQUIRED-FEATURES — G is named in the `required-features` of some target in
#       some workspace manifest, so it SELECTS whether that target is built at all.
#       (This is `duckdb-tests`' and `dhat-heap`'s real shape: zero cfg sites, and
#       load-bearing all the same.)
#
# CARGO NAMES A DEPENDENCY BY ITS KEY, NOT BY ITS PACKAGE NAME. A renamed workspace
# dependency (`bee = { path = "../b", package = "b" }`) is written `bee/bfeature`, so
# resolving edges by package name misses it, classifies it as EXTERNAL and
# auto-credits it under E2b without ever checking that the forwarded feature exists or
# does anything — a false PASS. The key -> member map is derived from each package's
# `dependencies` table in cargo metadata, and an edge naming a key the package does not
# declare is a NAMED FAIL rather than a guess.
#
# SOURCE OWNERSHIP COMES FROM METADATA TARGETS, NEVER FROM A DIRECTORY PREFIX. This
# workspace has the overlapping case for real: the member `cqlite-integration-tests`
# lives at `<root>/tests`, INSIDE the root package's own `tests/` directory, and the
# root package declares 24 test targets whose sources sit in that same directory. Under
# a longest-directory-prefix rule every one of those root-owned files was attributed to
# the nested member. Each target registers its exact `src_path` (which always wins) plus
# the tree under its directory — except a BUILD SCRIPT, whose directory IS the package
# root, so it registers as an exact file only. Ownership is a SET, deliberately: a file
# compiled as a target of two packages genuinely references both packages' features.
# A file inside a DEEPER member's package directory is not a shallower member's source.
#
# # THE ASYMMETRY IS THE WHOLE POINT
#
# CREDIT FLOWS UP FROM EFFECTS, NEVER DOWN FROM A PARENT. A feature is credited for
# what IT enables; it is credited NOTHING for being named by something else.
#
#   * An AGGREGATOR is legitimate: `all-compression = ["lz4", "snappy", "deflate",
#     "zstd"]` has no effect of its own, and each leaf pulls a dependency and has cfg
#     sites, so the aggregator passes through its closure.
#   * A LEAF named only by an aggregator is DEAD: `test-infrastructure =
#     ["test-schema-validation", ...]` named four leaves that had no effect anywhere.
#     Under a symmetric rule ("a feature is fine if it is referenced somewhere") the
#     aggregator would have laundered all four, which is exactly how they survived.
#
# Being ENUMERATED is likewise not an effect. The gate's own clippy feature lists,
# a workflow's `--features` argument and a doc table all NAME features without
# enabling anything, so they confer no credit — deleting a dead flag means cleaning
# those enumerations in the same diff, not treating them as evidence of life.
#
# # SOURCE OF TRUTH: `cargo metadata --no-deps`, NOT a textual manifest sweep
#
# Two measured reasons (issue #1698's census):
#
#   (a) cargo synthesises IMPLICIT features from optional dependencies that no
#       `[features]` block contains — 7 of them in this workspace
#       (`wasm-bindgen`/`js-sys`/`web-sys`, `ratatui`/`crossterm`,
#       `bollard`/`futures-util`). Each satisfies E2, so each passes; but a textual
#       scan cannot SEE them, and a guard must see everything it certifies.
#   (b) a `find -name Cargo.toml` sweep reaches manifests that are not workspace
#       members: three under `docs/reports/**` and `.agents/skills/**`, plus the
#       ORPHAN `cqlite-cli/tests/compatibility/Cargo.toml`, which cargo metadata does
#       not know and which cannot even resolve. Certifying a manifest cargo never
#       builds is noise at best and a false verdict at worst.
#
# `fuzz/` is its own EXCLUDED workspace (CLAUDE.md: the gate and default builds never
# compile it), so it is out of this guard's scope — its features are neither
# certified nor reported here.
#
# # FAIL-CLOSED, ALWAYS
#
# Every derivation failure is a NAMED FAIL naming the derivation: `cargo metadata`
# failing or emitting unparsable JSON, a member whose feature table cannot be read, a
# `pkg/feature` edge naming a member feature that does not exist, an unreadable
# source file, a source file that cannot be assigned to a member. NOTHING falls back
# to an empty feature set or an empty effect set — either would silently excuse every
# flag in the workspace, which is the standing rule this repository states as: never
# derive a pass from the ABSENCE of a bad signal. For the same reason a run that
# examined ZERO features is a FAIL, not a success.
#
# # NO OPT-OUT, BY DESIGN
#
# There is no bypass flag and no environment variable, and none may be added: an
# override is settable by the party it constrains, and a dead flag is ALWAYS
# deletable, so an escape hatch could only ever buy a vacuous green. The exemption
# set is hard-coded, tiny, and each entry carries its reason beside it (see
# EXEMPTIONS in the python section below).
#
# Exit 0 = every declared feature of every workspace member is load-bearing.
# 1 = a dead feature, or a refusal. 2 = usage error.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

usage() {
  cat <<'EOF'
Usage: scripts/ci/check-features-load-bearing.sh [--help]

Features-are-load-bearing guard for the cqlite workspace (issue #1698).

  (no flags)      Derive every feature declared by every workspace member from
                  `cargo metadata --no-deps`, and assert each one is LOAD-BEARING:
                  it, or something in its feature closure, has a cfg reference site
                  in its declaring package's sources, enables an optional
                  dependency, enables a feature of an external dependency, or is
                  named in some target's `required-features`.
  --help          This message.

Credit flows UP from effects, never DOWN from a parent: a leaf named only by an
aggregator is dead. Being enumerated (a workflow's `--features`, the gate's clippy
lists, a doc table) is not an effect.

There is deliberately NO bypass flag and NO environment opt-out: a dead flag is
always deletable, so an escape hatch could only buy a vacuous green.
EOF
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --help|-h) usage; exit 0 ;;
    *)
      echo "check-features-load-bearing.sh: unrecognized argument '$1'" >&2
      echo "" >&2
      usage >&2
      exit 2
      ;;
  esac
  shift
done

fail() {
  echo "" >&2
  echo "❌ features-load-bearing: $*" >&2
  exit 1
}

command -v cargo >/dev/null 2>&1 \
  || fail "cargo is not on PATH, so the feature set cannot be derived. This guard has no fallback derivation: a textual manifest sweep cannot see cargo's implicit features and reaches non-member manifests. Refusing to report a verdict it did not measure."
command -v python3 >/dev/null 2>&1 \
  || fail "python3 is not on PATH; the metadata reader cannot run. Refusing to report a verdict it did not measure."

WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/features-load-bearing.XXXXXX")"
cleanup() { rm -rf "$WORK_DIR"; return 0; }
trap cleanup EXIT

METADATA="$WORK_DIR/metadata.json"
# --no-deps: only workspace members are certified. --locked is deliberately NOT
# passed: this guard must not fail because a lockfile needs updating, that is a
# different check's job.
if ! (cd "$REPO_ROOT" && cargo metadata --no-deps --format-version 1) >"$METADATA" 2>"$WORK_DIR/metadata.err"; then
  echo "" >&2
  sed -n '1,40p' "$WORK_DIR/metadata.err" >&2
  fail "\`cargo metadata --no-deps\` FAILED (see above), so the declared-feature set could not be derived. Fail-closed: an unmeasured feature set is never an empty one. Remedy: fix the manifest error cargo reports."
fi

READER="$WORK_DIR/reader.py"
cat >"$READER" <<'PYEOF'
"""Feature-effect reader for check-features-load-bearing.sh (issue #1698).

Reads `cargo metadata --no-deps` JSON and the workspace's Rust sources, computes the
DIRECT EFFECTS of every declared feature of every workspace member, propagates credit
UP each feature's closure, and reports every feature with no effect anywhere in its
closure.

Refuses (exit 1 with a named cause) on any derivation it cannot complete. Never
returns a partial or empty effect set.
"""

import json
import os
import re
import sys

REPO_ROOT = os.path.realpath(sys.argv[1])
METADATA_PATH = sys.argv[2]


def fail(msg):
    sys.stderr.write("\n❌ features-load-bearing: " + msg + "\n")
    sys.exit(1)


# ---------------------------------------------------------------------------
# EXEMPTIONS — hard-coded, tiny, each with its reason. Not env-overridable.
#
#   "default": cargo gives `default` its meaning ITSELF — it is the feature set
#              selected when `--no-default-features` is absent — so `cfg(feature =
#              "default")` is meaningless and an EMPTY `default = []` is a legitimate,
#              load-bearing-by-cargo-semantics declaration (cqlite-flight,
#              cqlite-integration-tests, cqlite-node, format-validator and
#              sstabledump-validator all declare one). Requiring an effect of it would
#              red on correct input.
#
# Every other feature, in every member, is asserted. Adding an entry here requires a
# reason on this list, in this file, in the same diff.
# ---------------------------------------------------------------------------
EXEMPT_FEATURES = {
    "default": "cargo defines its meaning; `cfg(feature = \"default\")` is meaningless and an empty `default = []` is legitimate",
}

# Directory names never scanned for reference sites.
#   target/       — build output, not source.
#   .git/         — not source.
#   fuzz/         — its own EXCLUDED workspace (out of scope; see the script header).
#   node_modules/ — vendored JS.
SKIP_DIR_NAMES = {"target", ".git", "node_modules", "fuzz"}


# ---------------------------------------------------------------------------
# Comment stripping. STRING-AWARE, because a feature name IS a string literal:
# contents are KEPT, comments are dropped. A feature named only in a comment is not
# an effect — a commented-out cfg site compiles to nothing.
# ---------------------------------------------------------------------------
def strip_comments(text):
    out = []
    i = 0
    n = len(text)
    while i < n:
        c = text[i]
        # raw string: r"..." / r#"..."# / br#"..."#
        if c in "rb" and i + 1 < n:
            j = i
            if text[j] == "b" and j + 1 < n and text[j + 1] == "r":
                j += 1
            if text[j] == "r":
                k = j + 1
                hashes = 0
                while k < n and text[k] == "#":
                    hashes += 1
                    k += 1
                if k < n and text[k] == '"':
                    terminator = '"' + "#" * hashes
                    end = text.find(terminator, k + 1)
                    if end == -1:
                        out.append(text[i:])
                        break
                    out.append(text[i:end + len(terminator)])
                    i = end + len(terminator)
                    continue
        if c == '"':
            out.append(c)
            i += 1
            while i < n:
                if text[i] == "\\" and i + 1 < n:
                    out.append(text[i:i + 2])
                    i += 2
                    continue
                out.append(text[i])
                if text[i] == '"':
                    i += 1
                    break
                i += 1
            continue
        if c == "'":
            # char literal or lifetime; copy the next 1-3 chars verbatim, which is
            # enough to get past `'\''` and `'"'` without opening a string.
            if i + 1 < n and text[i + 1] == "\\":
                out.append(text[i:i + 4])
                i += 4
            else:
                out.append(text[i:i + 3])
                i += 3
            continue
        if c == "/" and i + 1 < n and text[i + 1] == "/":
            while i < n and text[i] != "\n":
                i += 1
            continue
        if c == "/" and i + 1 < n and text[i + 1] == "*":
            depth = 1
            i += 2
            while i < n and depth > 0:
                if text.startswith("/*", i):
                    depth += 1
                    i += 2
                elif text.startswith("*/", i):
                    depth -= 1
                    i += 2
                else:
                    if text[i] == "\n":
                        out.append("\n")
                    i += 1
            continue
        out.append(c)
        i += 1
    return "".join(out)


# ---------------------------------------------------------------------------
# CFG-CONTEXT extraction. `feature = "x"` ANYWHERE in a file is NOT an effect: it
# occurs in ordinary string literals, in `//!`/`///` doc text (this workspace has a
# live instance — `arbitrary_precision` appears in
# cqlite-ffi-common/src/json_number.rs only inside doc comments), in error messages
# and in `--features` argument strings. Crediting those credits a DEAD feature, which
# is the false-PASS direction, so the scan recognises actual cfg SYNTAX: a `cfg`,
# `cfg_attr` or `cfg!` token followed by a balanced parenthesised predicate. Nesting
# inside `all()`/`any()`/`not()` needs no special handling — it is inside the same
# balanced span — and `#[cfg]`/`#![cfg]`/`cfg_attr`/`cfg!` differ only in what
# precedes the token.
#
# For `cfg_attr(pred, attrs...)` ONLY the FIRST top-level argument is the predicate;
# the tail is attributes to apply, and a feature named there
# (`#[cfg_attr(docsrs, doc(cfg(feature = "x")))]`) gates nothing by itself. A nested
# real `cfg(...)` in that tail is still found, because the scan walks every cfg token
# in the file independently.
# ---------------------------------------------------------------------------
CFG_TOKEN_RE = re.compile(r'(?<![A-Za-z0-9_])(cfg_attr|cfg)[ \t\r\n]*(!?)[ \t\r\n]*\(')
FEATURE_PRED_RE = re.compile(r'(?<![A-Za-z0-9_])feature[ \t\r\n]*=[ \t\r\n]*"([^"\\]+)"')


def _skip_string(text, i):
    """i points at the opening quote of a `"..."` literal; return the index after it."""
    n = len(text)
    i += 1
    while i < n:
        if text[i] == "\\" and i + 1 < n:
            i += 2
            continue
        if text[i] == '"':
            return i + 1
        i += 1
    return n


def balanced_span(text, open_idx):
    """open_idx points at '('. Return (start, end) of the CONTENT, or None if unbalanced.

    String-aware: a `")"` inside a literal must not close the span.
    """
    n = len(text)
    depth = 0
    i = open_idx
    while i < n:
        c = text[i]
        if c == '"':
            i = _skip_string(text, i)
            continue
        if c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
            if depth == 0:
                return (open_idx + 1, i)
        i += 1
    return None


def first_top_level_arg(text, start, end):
    """The first comma-separated argument of a predicate span, at depth 0."""
    depth = 0
    i = start
    while i < end:
        c = text[i]
        if c == '"':
            i = _skip_string(text, i)
            continue
        if c in "([{":
            depth += 1
        elif c in ")]}":
            depth -= 1
        elif c == "," and depth == 0:
            return (start, i)
        i += 1
    return (start, end)


def cfg_feature_sites(text):
    """Yield (feature_name, offset) for every feature named in a real cfg predicate."""
    for m in CFG_TOKEN_RE.finditer(text):
        token = m.group(1)
        span = balanced_span(text, m.end() - 1)
        if span is None:
            # An unbalanced cfg( is not a site this scan can read. It is also not
            # compilable Rust, so it is left to rustc rather than guessed at.
            continue
        start, end = span
        if token == "cfg_attr":
            start, end = first_top_level_arg(text, start, end)
        for fm in FEATURE_PRED_RE.finditer(text, start, end):
            yield fm.group(1), fm.start()


# ---------------------------------------------------------------------------
# BUILD-SCRIPT ENV READS. cargo sets CARGO_FEATURE_<NAME> in a build script's
# environment, so reading one IS a cfg-equivalent effect — but only in a build script,
# and only as an actual env read. A bare textual `CARGO_FEATURE_X` (a doc comment, a
# message, a table of names) gates nothing, and crediting it is the same false-PASS
# route as the plain `feature = "x"` match above.
# ---------------------------------------------------------------------------
ENV_READ_RE = re.compile(
    r'(?:option_env!|(?:(?:std|core)::)?env::var(?:_os)?|(?<![A-Za-z0-9_:])var(?:_os)?)'
    r'[ \t\r\n]*\([ \t\r\n]*"CARGO_FEATURE_([A-Z0-9_]+)"'
)


def cargo_feature_env_name(feature):
    """cargo's build-script env spelling: uppercased, non-alphanumerics -> '_'."""
    return re.sub(r"[^A-Za-z0-9]", "_", feature).upper()


# ---------------------------------------------------------------------------
# 1) Metadata.
# ---------------------------------------------------------------------------
try:
    with open(METADATA_PATH, "r", encoding="utf-8") as fh:
        meta = json.load(fh)
except Exception as exc:  # noqa: BLE001 - any parse failure is fail-closed
    fail("`cargo metadata --no-deps` produced JSON this reader could not parse (%s). Refusing to report a verdict over an unreadable feature set." % exc)

packages = meta.get("packages")
member_ids = meta.get("workspace_members")
if not isinstance(packages, list) or not packages:
    fail("`cargo metadata --no-deps` reported NO packages. An empty package set is not a pass; refusing.")
if not isinstance(member_ids, list) or not member_ids:
    fail("`cargo metadata --no-deps` reported NO workspace_members. An empty member set is not a pass; refusing.")

member_id_set = set(member_ids)
members = {}
for pkg in packages:
    if pkg.get("id") not in member_id_set:
        continue
    name = pkg.get("name")
    manifest = pkg.get("manifest_path")
    feats = pkg.get("features")
    if not name or not manifest:
        fail("a workspace member in cargo metadata has no name or no manifest_path. Refusing to certify a member it cannot identify.")
    if not isinstance(feats, dict):
        fail("workspace member '%s' has no readable feature table in cargo metadata. Refusing to report a verdict over a member whose features could not be read." % name)
    pkg_dir = os.path.realpath(os.path.dirname(manifest))
    if not (pkg_dir == REPO_ROOT or pkg_dir.startswith(REPO_ROOT + os.sep)):
        fail("workspace member '%s' has its manifest OUTSIDE the repository root (%s). Refusing: its sources cannot be enumerated relative to this checkout." % (name, manifest))
    members[name] = {
        "name": name,
        "manifest": manifest,
        "manifest_rel": os.path.relpath(manifest, REPO_ROOT),
        "dir": pkg_dir,
        "features": feats,
        "targets": pkg.get("targets") or [],
        "dependencies": pkg.get("dependencies") or [],
        "refsites": {},
    }

if not members:
    fail("no workspace member could be reconstructed from cargo metadata. Refusing.")


# ---------------------------------------------------------------------------
# 2) DEPENDENCY KEYS. Cargo's feature syntax names a dependency by its KEY — the
#    `rename` when the manifest renames it (`bee = { package = "b" }` is written
#    `bee/bfeat`, never `b/bfeat`) — so resolving `pkg/feature` edges by PACKAGE NAME
#    misses a renamed workspace member and silently classifies it as EXTERNAL, which
#    auto-credits the edge without ever checking that the forwarded feature exists or
#    has any effect: a false PASS. The key -> member map is built here, from metadata.
# ---------------------------------------------------------------------------
for name, rec in members.items():
    keys = {}
    for dep in rec["dependencies"]:
        dname = dep.get("name")
        if not dname:
            fail("member '%s' has a dependency with no name in cargo metadata. Refusing to resolve feature edges against an unreadable dependency table." % name)
        key = dep.get("rename") or dname
        # A local (path/workspace) dependency on a member is the only thing whose
        # features this guard can follow; a registry crate that happens to share a
        # member's name is external.
        is_local = dep.get("path") is not None or dep.get("source") is None
        entry = keys.setdefault(key, {"package": dname, "member": None, "optional": False})
        if entry["package"] != dname:
            fail("member '%s' uses the dependency key '%s' for two different packages ('%s' and '%s') in cargo metadata. Refusing to resolve feature edges through an ambiguous key." % (name, key, entry["package"], dname))
        if dep.get("optional"):
            entry["optional"] = True
        if is_local and dname in members:
            entry["member"] = dname
    rec["dep_keys"] = keys


# ---------------------------------------------------------------------------
# 3) SOURCE OWNERSHIP, derived from metadata TARGETS — never from a package-directory
#    prefix. This workspace has the overlapping case FOR REAL: the member
#    `cqlite-integration-tests` lives at `<root>/tests`, INSIDE the root package's own
#    `tests/` directory, and the root package declares 24 test targets whose sources
#    sit in that same directory. Under a longest-directory-prefix rule every one of
#    those root-owned files was attributed to the nested member — cfg sites credited to
#    the wrong package, in both the false-PASS and false-FAIL directions.
#
#    Ownership is a SET, and that is deliberate rather than a tie-break: a file
#    compiled as a target of TWO packages (which is exactly what `<root>/tests/*.rs`
#    is here) genuinely references BOTH packages' features, so both are credited.
#
#    Two registrations per target:
#      * its EXACT src_path, which always wins; and
#      * the TREE under `dirname(src_path)`, which is how a target reaches its module
#        files (`tests/common/mod.rs`, `src/**`) — EXCEPT for a custom-build target,
#        whose dirname is the PACKAGE ROOT. Registering that would hand a package the
#        whole of its own directory again, nested members and all, so a build script
#        is registered as an exact file only.
#
#    Nested-member exclusion: a file inside a DEEPER member's package directory is not
#    the shallower member's source, unless it is literally one of the shallower
#    member's target files.
# ---------------------------------------------------------------------------
TREELESS_KINDS = {"custom-build"}
exact_owners = {}    # realpath -> set(member names)
tree_owners = []     # (tree_dir, member name)
buildscript_files = {}   # realpath -> set(member names)

for name, rec in members.items():
    for target in rec["targets"]:
        sp = target.get("src_path")
        kinds = target.get("kind") or []
        if not sp:
            fail("target '%s' of member '%s' has no src_path in cargo metadata. Refusing to derive source ownership from an unreadable target." % (target.get("name"), name))
        sp = os.path.realpath(sp)
        exact_owners.setdefault(sp, set()).add(name)
        if set(kinds) & TREELESS_KINDS:
            buildscript_files.setdefault(sp, set()).add(name)
            continue
        tree_owners.append((os.path.dirname(sp), name))

tree_owners.sort(key=lambda t: len(t[0]), reverse=True)
member_dirs = sorted(((rec["dir"], name) for name, rec in members.items()), key=lambda t: len(t[0]), reverse=True)


def deepest_member_dir(path):
    for pkg_dir, name in member_dirs:
        if path == pkg_dir or path.startswith(pkg_dir + os.sep):
            return pkg_dir, name
    return None, None


def owners_of(path):
    owners = set(exact_owners.get(path, ()))
    deep_dir, _deep_name = deepest_member_dir(path)
    for tree_dir, name in tree_owners:
        if name in owners:
            continue
        if path == tree_dir or path.startswith(tree_dir + os.sep):
            if deep_dir is not None and len(members[name]["dir"]) < len(deep_dir):
                # The file lives inside a DEEPER member's package directory; it is
                # that member's source, not this one's.
                continue
            owners.add(name)
    return owners


# ---------------------------------------------------------------------------
# 4) E1 — reference sites, per OWNING package.
# ---------------------------------------------------------------------------
scanned_files = 0
for dirpath, dirnames, filenames in os.walk(REPO_ROOT):
    dirnames[:] = [d for d in dirnames if d not in SKIP_DIR_NAMES]
    for fname in filenames:
        if not fname.endswith(".rs"):
            continue
        full = os.path.realpath(os.path.join(dirpath, fname))
        owners = owners_of(full)
        if not owners:
            # Not a source file of any workspace-member target: a non-member crate's
            # source (the measurement harnesses under docs/reports/**), or a stray .rs
            # no target reaches. Nothing to certify.
            continue
        try:
            with open(full, "r", encoding="utf-8", errors="replace") as fh:
                text = fh.read()
        except OSError as exc:
            fail("could not read the source file %s (%s) while collecting feature reference sites. Refusing to report a verdict over sources it could not read." % (os.path.relpath(full, REPO_ROOT), exc))
        scanned_files += 1
        stripped = strip_comments(text)
        rel = os.path.relpath(full, REPO_ROOT)
        sites = list(cfg_feature_sites(stripped))
        env_sites = []
        if full in buildscript_files:
            env_sites = [(m.group(1), m.start()) for m in ENV_READ_RE.finditer(stripped)]
        for owner in owners:
            record = members[owner]["refsites"]
            for feat, off in sites:
                if feat not in record:
                    record[feat] = "%s:%d" % (rel, stripped.count("\n", 0, off) + 1)
            if not env_sites or owner not in buildscript_files.get(full, ()):
                continue
            for env, off in env_sites:
                for feat in members[owner]["features"]:
                    if cargo_feature_env_name(feat) == env and feat not in record:
                        record[feat] = "%s:%d (CARGO_FEATURE_%s)" % (rel, stripped.count("\n", 0, off) + 1, env)

if scanned_files == 0:
    fail("NOT ONE Rust source file of a workspace-member target could be found, so no reference site could possibly have been observed. A positive verdict requires an affirmative measurement; refusing to pass over an empty scan.")

# ---------------------------------------------------------------------------
# 5) E3 — required-features, across every member's targets.
# ---------------------------------------------------------------------------
required = set()
for name, rec in members.items():
    for target in rec["targets"]:
        rf = target.get("required-features") or []
        if not isinstance(rf, list):
            fail("target '%s' of member '%s' has a non-list required-features in cargo metadata. Refusing to guess at its shape." % (target.get("name"), name))
        for entry in rf:
            if not isinstance(entry, str) or not entry:
                fail("target '%s' of member '%s' has an unreadable required-features entry. Refusing." % (target.get("name"), name))
            if "/" in entry:
                keypart, _, feat = entry.partition("/")
                key = keypart[:-1] if keypart.endswith("?") else keypart
                info = rec["dep_keys"].get(key)
                if info is None or info["member"] is None:
                    continue
                owner = info["member"]
            else:
                owner, feat = name, entry
            if feat not in members[owner]["features"]:
                fail("target '%s' of member '%s' names required-features '%s', but member '%s' declares no feature '%s'. Refusing to compute a closure over a feature that does not exist." % (target.get("name"), name, entry, owner, feat))
            required.add((owner, feat))

# ---------------------------------------------------------------------------
# 6) DIRECT EFFECTS and CLOSURE EDGES.
#
#    A WEAK edge (`dep?/feature`) is NOT an effect on its own: cargo does nothing with
#    it unless that optional dependency is activated by something else. Crediting it
#    unconditionally credits a feature that changes nothing — a false PASS — so the `?`
#    is preserved here and the edge is evaluated per ORIGIN in step 7, live only when
#    the dependency is activated somewhere in that origin's closure.
#
#    A NON-weak `dep/feature` on an OPTIONAL dependency also ACTIVATES it (cargo's
#    documented behaviour), so it is both an effect and an activation.
# ---------------------------------------------------------------------------
uncond = {}      # (pkg, feat) -> True when an unconditional direct effect exists
edges = {}       # (pkg, feat) -> [(kind, key, target_node_or_feat, weak)]
own_deps = {}    # (pkg, feat) -> set of dependency KEYS this feature activates
ext_edges = {}   # (pkg, feat) -> [(key, dep_feature, weak)]
all_nodes = []

for name, rec in members.items():
    for feat, deplist in sorted(rec["features"].items()):
        node = (name, feat)
        all_nodes.append(node)
        if not isinstance(deplist, list):
            fail("feature '%s' of member '%s' has a non-list dependency list in cargo metadata. Refusing to guess at its shape." % (feat, name))
        out = []
        acts = set()
        exts = []
        effect = False
        for entry in deplist:
            if not isinstance(entry, str) or not entry:
                fail("feature '%s' of member '%s' has an unreadable entry in its dependency list. Refusing." % (feat, name))
            if entry.startswith("dep:"):
                key = entry[4:]
                if key not in rec["dep_keys"]:
                    fail("feature '%s' of member '%s' enables the optional dependency '%s', but member '%s' declares no dependency with the key '%s'. Refusing to credit an effect it could not resolve." % (feat, name, entry, name, key))
                acts.add(key)
                effect = True
                continue
            if "/" in entry:
                keypart, _, dfeat = entry.partition("/")
                weak = keypart.endswith("?")
                key = keypart[:-1] if weak else keypart
                info = rec["dep_keys"].get(key)
                if info is None:
                    fail("feature '%s' of member '%s' enables '%s', but member '%s' declares no dependency with the key '%s'. Cargo's feature syntax names a dependency by its KEY (its `rename`, when renamed); refusing to credit an edge it could not resolve." % (feat, name, entry, name, key))
                if not weak and info["optional"]:
                    # `dep/feat` on an optional dependency activates it too.
                    acts.add(key)
                    effect = True
                if info["member"] is not None:
                    tgt = info["member"]
                    if dfeat not in members[tgt]["features"]:
                        fail("feature '%s' of member '%s' enables '%s', but workspace member '%s' (dependency key '%s') declares no feature '%s'. Refusing to compute a closure over a feature that does not exist." % (feat, name, entry, tgt, key, dfeat))
                    out.append(("member", key, (tgt, dfeat), weak))
                else:
                    exts.append((key, dfeat, weak))
                    if not weak:
                        effect = True
                continue
            if entry not in rec["features"]:
                fail("feature '%s' of member '%s' enables '%s', which member '%s' does not declare as a feature. Refusing to compute a closure over a feature that does not exist." % (feat, name, entry, name))
            out.append(("feature", None, (name, entry), False))
        if feat in rec["refsites"]:
            effect = True
        if node in required:
            effect = True
        edges[node] = out
        own_deps[node] = acts
        ext_edges[node] = exts
        if effect:
            uncond[node] = True

if not all_nodes:
    fail("no workspace member declares ANY feature, so this assert examined nothing. A positive verdict requires an affirmative measurement; refusing to pass.")

# ---------------------------------------------------------------------------
# 7) Propagate credit UP the closure, PER ORIGIN: a feature is load-bearing iff itself
#    or ANY node reachable from it (what it ENABLES) has an effect. Never the reverse —
#    credit flows up from effects, never down from a parent, which is what makes a leaf
#    named only by an aggregator detectable.
#
#    Computed per origin because weak-edge liveness depends on WHICH origin is being
#    enabled: `a = ["x?/f"]` alone activates nothing, while `b = ["dep:x", "x?/f"]`
#    makes the same edge live. The fixpoint grows both the reachable set and the set of
#    activated dependency keys until neither changes.
# ---------------------------------------------------------------------------
def analyse(origin):
    reach = {origin}
    acts = set(own_deps[origin])
    changed = True
    while changed:
        changed = False
        for n in list(reach):
            for kind, key, tgt, weak in edges[n]:
                if weak and key not in acts:
                    continue
                if tgt not in reach:
                    reach.add(tgt)
                    changed = True
                for k in own_deps[tgt]:
                    if k not in acts:
                        acts.add(k)
                        changed = True
    return reach, acts


def load_bearing(origin):
    reach, acts = analyse(origin)
    for n in reach:
        if n in uncond:
            return True
        for key, _dfeat, weak in ext_edges[n]:
            if not weak or key in acts:
                return True
    return False


# ---------------------------------------------------------------------------
# 8) Verdict.
# ---------------------------------------------------------------------------
def declaration_line(manifest_path, feature):
    """Line of `feature = [...]` inside [features]; None for a cargo-implicit one."""
    try:
        with open(manifest_path, "r", encoding="utf-8") as fh:
            lines = fh.read().splitlines()
    except OSError as exc:
        fail("could not read the manifest %s (%s) to locate a feature declaration. Refusing." % (manifest_path, exc))
    in_features = False
    pat = re.compile(r'^\s*(?:"%s"|%s)\s*=' % (re.escape(feature), re.escape(feature)))
    for idx, line in enumerate(lines, start=1):
        s = line.strip()
        if s.startswith("["):
            in_features = s in ("[features]", "[ features ]")
            continue
        if in_features and pat.match(line):
            return idx
    return None


dead = []
exempt_count = 0
for node in all_nodes:
    if node[1] in EXEMPT_FEATURES:
        exempt_count += 1
        continue
    if not load_bearing(node):
        dead.append(node)

total = len(all_nodes)
asserted = total - exempt_count
if asserted == 0:
    fail("every one of the %d declared features is EXEMPT, so this assert examined nothing. A positive verdict requires an affirmative measurement; refusing to pass." % total)

if dead:
    sys.stderr.write("\n❌ features-load-bearing: %d declared feature(s) are DEAD — enabling them changes nothing that is compiled, linked or selected:\n\n" % len(dead))
    for pkg, feat in sorted(dead):
        manifest_rel = members[pkg]["manifest_rel"]
        line = declaration_line(members[pkg]["manifest"], feat)
        where = "%s:%d" % (manifest_rel, line) if line else "%s (cargo-implicit)" % manifest_rel
        sys.stderr.write("    %s  %s   [%s]\n" % (where, feat, pkg))
    sys.stderr.write("""
    Each has NO effect anywhere in its own closure: no cfg reference site in its
    DECLARING package's sources, no optional dependency, no LIVE external dependency
    feature, and no target's required-features names it. Being NAMED confers nothing:
    an aggregator that lists a leaf, a workflow `--features` argument, the gate's
    clippy enumerations and a doc table all name features without enabling anything.
    Nor does a WEAK `dep?/feature` edge whose optional dependency nothing activates.

    Remedy, one of:
      * DELETE the feature from its manifest (and every enumeration that names it:
        the gate's clippy feature lists in scripts/agent-gate.sh, workflow
        `--features` arguments, the CLAUDE.md feature table, docs); or
      * GIVE it an effect: a `#[cfg(feature = "...")]` site in the declaring
        package's sources, an optional dependency, or a target `required-features`.

    Issue #1698 (epic #1685, config honesty).
""")
    sys.exit(1)

# AFFIRMATIVE success line: a count, never a bare "OK". Every element of it is
# something the guard can only know AFTER deriving the member set from cargo, walking
# the target sources and computing every closure. The `features-load-bearing` component
# of scripts/agent-gate.sh matches this line WHOLE.
print(
    "features-load-bearing: %d/%d declared features load-bearing across %d workspace manifests "
    "(%d exempt: %s); %d Rust source files scanned for reference sites"
    % (asserted, asserted, len(members), exempt_count, ", ".join(sorted(EXEMPT_FEATURES)), scanned_files)
)
PYEOF

if ! python3 "$READER" "$REPO_ROOT" "$METADATA"; then
  exit 1
fi
