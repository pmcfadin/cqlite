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
#   E1  REFERENCE SITE — G's name appears as a cfg predicate spelling
#       (`feature = "G"`, in `cfg`/`cfg_attr`/`cfg!`/`#![cfg]`, at any nesting depth
#       inside `all()`/`any()`/`not()` — the spelling is the same either way), or as
#       a `CARGO_FEATURE_G` build-script environment read, in the sources OF THE
#       PACKAGE THAT DECLARES G (src/, tests/, benches/, examples/, build.rs).
#   E2  OPTIONAL DEPENDENCY — G's dep list enables an optional dependency (`dep:x`).
#       The "bare optional-dep name" spelling (`wasm = ["wasm-bindgen", ...]`) is
#       covered by the closure, because cargo SYNTHESISES an implicit feature per
#       optional dep whose own dep list is exactly `["dep:x"]`.
#   E2b DEPENDENCY FEATURE — G's dep list enables a feature of a NON-member
#       dependency (`opentelemetry_sdk?/testing`). Enabling a feature of an external
#       crate demonstrably changes that crate's compiled code, so it is load-bearing
#       by definition. This guard cannot audit non-workspace sources, and does not
#       need to: the effect is established by the edge itself.
#   E3  REQUIRED-FEATURES — G is named in the `required-features` of some target in
#       some workspace manifest, so it SELECTS whether that target is built at all.
#       (This is `duckdb-tests`' and `dhat-heap`'s real shape: zero cfg sites, and
#       load-bearing all the same.)
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


FEATURE_PRED_RE = re.compile(r'feature\s*=\s*"([^"\\]+)"')
CARGO_FEATURE_ENV_RE = re.compile(r'CARGO_FEATURE_([A-Z0-9_]+)')


def env_name_candidates(feature):
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
members = {}          # name -> record
by_dir = []           # (dir, name) for source assignment
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
        "refsites": {},   # feature -> "relpath:line"
    }
    by_dir.append((pkg_dir, name))

if not members:
    fail("no workspace member could be reconstructed from cargo metadata. Refusing.")

# Longest-prefix wins, so a nested member (tests/, tools/*, bindings/*) claims its own
# sources rather than the root package claiming the whole tree.
by_dir.sort(key=lambda t: len(t[0]), reverse=True)


def owner_of(path):
    for pkg_dir, name in by_dir:
        if path == pkg_dir or path.startswith(pkg_dir + os.sep):
            return name
    return None


# ---------------------------------------------------------------------------
# 2) E1 — reference sites, per DECLARING package.
# ---------------------------------------------------------------------------
scanned_files = 0
for dirpath, dirnames, filenames in os.walk(REPO_ROOT):
    dirnames[:] = [d for d in dirnames if d not in SKIP_DIR_NAMES]
    for fname in filenames:
        if not fname.endswith(".rs"):
            continue
        full = os.path.join(dirpath, fname)
        owner = owner_of(os.path.realpath(full))
        if owner is None:
            # Not under any workspace member: a non-member crate's source (e.g. the
            # measurement harnesses under docs/reports/**). Nothing to certify.
            continue
        try:
            with open(full, "r", encoding="utf-8", errors="replace") as fh:
                text = fh.read()
        except OSError as exc:
            fail("could not read the source file %s (%s) while collecting feature reference sites. Refusing to report a verdict over sources it could not read." % (os.path.relpath(full, REPO_ROOT), exc))
        scanned_files += 1
        stripped = strip_comments(text)
        rel = os.path.relpath(full, REPO_ROOT)
        record = members[owner]["refsites"]
        for m in FEATURE_PRED_RE.finditer(stripped):
            name = m.group(1)
            if name not in record:
                line = stripped.count("\n", 0, m.start()) + 1
                record[name] = "%s:%d" % (rel, line)
        for m in CARGO_FEATURE_ENV_RE.finditer(stripped):
            env = m.group(1)
            for feat in members[owner]["features"]:
                if env_name_candidates(feat) == env and feat not in record:
                    line = stripped.count("\n", 0, m.start()) + 1
                    record[feat] = "%s:%d (CARGO_FEATURE_%s)" % (rel, line, env)

if scanned_files == 0:
    fail("NOT ONE Rust source file under a workspace member could be found, so no reference site could possibly have been observed. A positive verdict requires an affirmative measurement; refusing to pass over an empty scan.")

# ---------------------------------------------------------------------------
# 3) E3 — required-features, across every member's targets.
# ---------------------------------------------------------------------------
required_by = {}      # (pkg, feature) -> "manifest_rel target"
for name, rec in members.items():
    for target in rec["targets"]:
        rf = target.get("required-features") or []
        if not isinstance(rf, list):
            fail("target '%s' of member '%s' has a non-list required-features in cargo metadata. Refusing to guess at its shape." % (target.get("name"), name))
        for entry in rf:
            if not isinstance(entry, str) or not entry:
                fail("target '%s' of member '%s' has an unreadable required-features entry. Refusing." % (target.get("name"), name))
            if "/" in entry:
                dep, _, feat = entry.partition("/")
                dep = dep.rstrip("?")
                owner = dep if dep in members else None
            else:
                owner, feat = name, entry
            if owner is None:
                # required-features naming a non-member package's feature: an external
                # effect, nothing of ours to credit.
                continue
            if feat not in members[owner]["features"]:
                fail("target '%s' of member '%s' names required-features '%s', but member '%s' declares no feature '%s'. Refusing to compute a closure over a feature that does not exist." % (target.get("name"), name, entry, owner, feat))
            required_by.setdefault((owner, feat), "%s (target `%s`)" % (members[owner]["manifest_rel"], target.get("name")))

# ---------------------------------------------------------------------------
# 4) DIRECT EFFECTS per (package, feature), and the closure edges.
# ---------------------------------------------------------------------------
direct = {}       # (pkg, feat) -> effect description
edges = {}        # (pkg, feat) -> [(pkg, feat), ...]
all_nodes = []
for name, rec in members.items():
    for feat, deplist in sorted(rec["features"].items()):
        node = (name, feat)
        all_nodes.append(node)
        if not isinstance(deplist, list):
            fail("feature '%s' of member '%s' has a non-list dependency list in cargo metadata. Refusing to guess at its shape." % (feat, name))
        why = None
        out = []
        for entry in deplist:
            if not isinstance(entry, str) or not entry:
                fail("feature '%s' of member '%s' has an unreadable entry in its dependency list. Refusing." % (feat, name))
            if entry.startswith("dep:"):
                if why is None:
                    why = "E2 enables optional dependency `%s`" % entry
                continue
            if "/" in entry:
                dep, _, dfeat = entry.partition("/")
                dep = dep.rstrip("?")
                if dep in members:
                    if dfeat not in members[dep]["features"]:
                        fail("feature '%s' of member '%s' enables '%s', but workspace member '%s' declares no feature '%s'. Refusing to compute a closure over a feature that does not exist." % (feat, name, entry, dep, dfeat))
                    out.append((dep, dfeat))
                else:
                    if why is None:
                        why = "E2b enables feature `%s` of external dependency `%s`" % (dfeat, dep)
                continue
            # Bare name: a feature of THIS package (explicit or cargo-implicit).
            if entry not in rec["features"]:
                fail("feature '%s' of member '%s' enables '%s', which member '%s' does not declare as a feature. Refusing to compute a closure over a feature that does not exist." % (feat, name, entry, name))
            out.append((name, entry))
        # E1 first when present: a reference site is the most informative effect to
        # report. Order affects only the WORDING; the verdict is the same either way.
        site = rec["refsites"].get(feat)
        if site is not None:
            why = "E1 reference site at %s" % site
        elif why is None and node in required_by:
            why = "E3 named in required-features of %s" % required_by[node]
        edges[node] = out
        if why is not None:
            direct[node] = why

if not all_nodes:
    fail("no workspace member declares ANY feature, so this assert examined nothing. A positive verdict requires an affirmative measurement; refusing to pass.")

# ---------------------------------------------------------------------------
# 5) Propagate credit UP the closure: a node is load-bearing iff itself or ANY node
#    reachable from it (what it ENABLES) has a direct effect. Never the reverse.
# ---------------------------------------------------------------------------
verdict = {}


def resolve(node, stack):
    if node in verdict:
        return verdict[node]
    if node in stack:
        # A feature cycle: cargo rejects these, but do not hang on one.
        return None
    if node in direct:
        verdict[node] = direct[node]
        return verdict[node]
    stack.add(node)
    found = None
    for nxt in edges.get(node, []):
        got = resolve(nxt, stack)
        if got is not None:
            found = "via `%s/%s`: %s" % (nxt[0], nxt[1], got)
            break
    stack.discard(node)
    if found is not None:
        verdict[node] = found
        return found
    verdict[node] = None
    return None


sys.setrecursionlimit(10000)
for node in all_nodes:
    resolve(node, set())

# ---------------------------------------------------------------------------
# 6) Verdict.
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
    pkg, feat = node
    if feat in EXEMPT_FEATURES:
        exempt_count += 1
        continue
    if verdict.get(node) is None:
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
    DECLARING package's sources, no optional dependency, no external dependency
    feature, and no target's required-features names it. Being NAMED confers nothing:
    an aggregator that lists a leaf, a workflow `--features` argument, the gate's
    clippy enumerations and a doc table all name features without enabling anything.

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
# the sources, and computing every closure. The `features-load-bearing` component of
# scripts/agent-gate.sh matches this line WHOLE.
print(
    "features-load-bearing: %d/%d declared features load-bearing across %d workspace manifests "
    "(%d exempt: %s); %d Rust source files scanned for reference sites"
    % (asserted, asserted, len(members), exempt_count, ", ".join(sorted(EXEMPT_FEATURES)), scanned_files)
)
PYEOF

if ! python3 "$READER" "$REPO_ROOT" "$METADATA"; then
  exit 1
fi
