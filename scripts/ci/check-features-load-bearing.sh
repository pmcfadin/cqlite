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
#   E1  REFERENCE SITE — G is named as a feature in a REAL, ANCHORED cfg gate in the
#       sources of the package that declares G, or read as `CARGO_FEATURE_G` from the
#       environment by that package's BUILD SCRIPT.
#
#       ONE LEXICAL PASS, THEN POSITIONAL MATCHING. Text matching over Rust source
#       credits non-effects, and narrowing the pattern once per instance is choosing a
#       rarer delimiter — the shape CLAUDE.md rules against (#3312: remove the
#       channel, do not narrow the delimiter). Round 1 of review found string
#       literals; round 2 found `doc(cfg(...))`, raw strings and a bare `var()`. All
#       one class. So each file is lexed ONCE into (a) a `code` text with every
#       comment byte blanked and every STRING-LITERAL byte — delimiters, prefixes,
#       any raw-hash count, byte and C strings — replaced by a sentinel, and (b) a
#       table of those literals by offset. Structure is matched only against `code`,
#       and a feature NAME is read from the table at the exact offset where
#       `feature =` ends. Nothing inside a comment or a string can look like code.
#
#       ONLY ANCHORED HEADS COUNT: `#[cfg(`, `#![cfg(`, `cfg!(`, and the CONDITION
#       (first top-level argument) of `#[cfg_attr(` / `#![cfg_attr(`. A `cfg(...)`
#       nested anywhere else is deliberately NOT a site — most importantly
#       `#[cfg_attr(docsrs, doc(cfg(feature = "x")))]`, which is DOCUMENTATION and
#       gates no compilation. `all()`/`any()`/`not()` need no special handling: they
#       are inside the head's own balanced span.
#
#       THE BUILD-SCRIPT ROUTE is an env read through a genuine API, in three
#       ANCHORED spellings: a full `std::env::var` / `core::env::var` path (leading
#       `::` allowed, but NOT `my_std::env::var`); a bare `env::var` ONLY when the file
#       proves the MODULE binding (`use std::env;`, braced lists included, and NOT
#       `my_env::var`); and a bare `var`/`var_os` ONLY when the file proves the
#       FUNCTION binding (`use std::env::var`). `env` and `var` are ordinary
#       identifiers — an unanchored match accepted a LOCAL `mod env` and even the
#       `env::var` suffix of `my_env::var(...)`, each crediting a dead feature.
#       `option_env!` is not accepted: cargo documents these variables for build-script
#       EXECUTION, and claiming a compile-time read would assert something unverified.
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
#       by the edge itself. TWO KINDS OF EDGE ARE NOT EFFECTS, though:
#         * A WEAK edge (`x?/feature`) — cargo does nothing with it unless the
#           optional dependency `x` is activated by something else, so it is credited
#           only when `x` is activated somewhere in the ORIGIN's closure.
#           `observability-testing = ["observability", "opentelemetry_sdk?/testing"]`
#           is live because `observability` activates that dep; `["x?/f"]` alone is
#           dead.
#         * A REDUNDANT edge — one that enables what the DEPENDENCY DECLARATION
#           already enables AND that activates nothing. ACTIVATION IS JUDGED FIRST: a
#           non-weak edge to an OPTIONAL dependency pulls that dependency into the
#           build, which is an effect however redundant the forwarded feature is, so
#           `f = ["foo/x"]` beside `foo = { optional = true, features = ["x"] }` stays
#           LOAD-BEARING. Getting that order wrong reported a live feature as dead. `foo = ["serde/derive"]` beside
#           `serde = { features = ["derive"] }` changes nothing, and neither does
#           `foo = ["dep/default"]` on a dependency that already uses default
#           features. Cargo metadata resolves workspace-dependency inheritance, so an
#           inherited `features = [...]` is visible here. Judged over ALL declarations
#           under the key (normal, dev, build, per-target): credit is withheld only
#           when every one already enables it, the direction that cannot invent a
#           false FAIL. Only DIRECT redundancy is decided — see the declared residual.
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
# # THE DECLARED RESIDUAL — printed on every success, never implied away
#
# This scan is LEXICAL, not a compiler, so the success line ends with
# `cfg-site detection: lexical, NON-EXHAUSTIVE` naming the two things it does not
# decide. A lane that omits coverage silently is indistinguishable from one that covers
# it, and a positive verdict must rest on an affirmative measurement:
#
#   * A cfg produced by MACRO EXPANSION is not seen. A feature name assembled by a
#     macro (or emitted by a `macro_rules!` body that this scan reads as tokens rather
#     than expands) has no textual `feature = "NAME"` for the scan to find. Direction:
#     it would report such a feature DEAD — a false FAIL, which is loud.
#   * An ORPHAN `.rs` file under a target's source directory — one no `mod` chain
#     reaches from the target root — is scanned as if it were compiled, so a cfg gate
#     in dead code can credit a dead feature. Deciding it means resolving Rust's module
#     graph (`mod`, `#[path]`, `#[cfg]`-gated `mod`, nested inline modules) from bash,
#     which is the UNBOUNDED-PARSING problem this repo has already paid for once and
#     removed: #1712 deleted the rustdoc/public-API half of `pub-surface` precisely
#     because a scanner that must find declarations anywhere in arbitrary source cannot
#     abstain. Not worth that cost for a case that needs dead code to be committed AND
#     to name an otherwise-dead feature. Direction: such a file is SCANNED — a residual
#     false PASS. Pinned by a fixture in
#     scripts/tests/test_features_load_bearing_guard.sh, so this declaration cannot
#     drift from the code: a declaration nobody tests is a comment.
#   * An INDIRECTLY redundant dependency-feature edge is not detected: `dep/x` where
#     the declaration enables some feature that itself enables `x`. Deciding it needs
#     the dependency's own feature table, which `--no-deps` does not carry for external
#     crates. Direction: such an edge is CREDITED — a residual false PASS, declared
#     here rather than hidden.
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
                  it, or something in its feature closure, has a REAL cfg site in
                  its declaring package's sources (`#[cfg(`, `#![cfg(`, `cfg!(`, or
                  the CONDITION of `#[cfg_attr(` — read after ONE lexical pass that
                  blanks comments and all string literals, so nothing inside a
                  string, a raw string or a comment can be mistaken for code),
                  enables an optional dependency, enables a feature of an external
                  dependency (weak and already-enabled edges excluded — they change
                  nothing), or is named in some target's `required-features`.
  --help          This message.

The success line DECLARES the residual of the method
(`cfg-site detection: lexical, NON-EXHAUSTIVE`): a cfg produced by MACRO EXPANSION is
not seen, and an INDIRECTLY redundant dependency-feature edge is not detected.

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
    "default": "cargo defines its meaning; a cfg on it is meaningless and an empty default = [] is legitimate",
}

# Directory names never scanned for reference sites.
#   target/       — build output, not source.
#   .git/         — not source.
#   fuzz/         — its own EXCLUDED workspace (out of scope; see the script header).
#   node_modules/ — vendored JS.
SKIP_DIR_NAMES = {"target", ".git", "node_modules", "fuzz"}

# THE DECLARED RESIDUAL, printed on every success. See the script header: this scan is
# LEXICAL, and two things it cannot decide are named rather than implied away.
RESIDUAL_NOTE = ("cfg-site detection: lexical, NON-EXHAUSTIVE "
                 "(a cfg produced by MACRO EXPANSION is not seen; an ORPHAN .rs file under a "
                 "target's source dir, not reachable from the target root, is scanned as if "
                 "compiled; an INDIRECTLY redundant dependency-feature edge is not detected)")

STR_SENTINEL = "\x01"   # every byte of a string literal, in the cleaned text
IDENT_CHARS = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_")


# ---------------------------------------------------------------------------
# ONE LEXICAL PREPROCESSING PASS (roborev job 52, findings 1-3 are ONE class).
#
# Text matching over Rust source credits non-effects, and narrowing the pattern once
# per instance is choosing a rarer delimiter — the shape CLAUDE.md rules against
# (#3312: remove the channel). So the file is lexed ONCE, here, into:
#
#   code    — the same length as the input, with every COMMENT byte replaced by a space
#             (newlines kept, so line numbers stay exact) and every STRING-LITERAL byte
#             (delimiters, prefixes and all) replaced by STR_SENTINEL. Structure is
#             matched ONLY against this text, so nothing inside a comment or a string
#             — raw, byte, C or plain, any hash count — can ever look like code.
#   strings — the literals, keyed by their START offset in `code`, with their content.
#
# A feature NAME is a string literal, so it cannot simply be stripped: the name is read
# from `strings` at the exact offset where `feature =` ends. That is what makes the
# scan positional rather than textual — a `feature = "x"` INSIDE a string or comment
# contributes no `feature` token to `code` at all, and a real one is only accepted when
# the literal begins exactly where the predicate expects a value.
#
# CHAR LITERALS are blanked too, because `'"'` would otherwise open a string. A
# LIFETIME (`'a`) is not a literal; its quote is blanked and its name left alone, which
# is inert either way.
# ---------------------------------------------------------------------------
def lex(text):
    n = len(text)
    code = list(text)
    strings = {}
    i = 0
    while i < n:
        c = text[i]
        # --- comments -------------------------------------------------------
        if c == "/" and i + 1 < n and text[i + 1] == "/":
            while i < n and text[i] != "\n":
                code[i] = " "
                i += 1
            continue
        if c == "/" and i + 1 < n and text[i + 1] == "*":
            depth = 0
            while i < n:
                if text.startswith("/*", i):
                    depth += 1
                    code[i] = " "
                    code[i + 1] = " "
                    i += 2
                    continue
                if text.startswith("*/", i):
                    depth -= 1
                    code[i] = " "
                    code[i + 1] = " "
                    i += 2
                    if depth == 0:
                        break
                    continue
                if text[i] != "\n":
                    code[i] = " "
                i += 1
            continue
        # --- string literals, with every prefix Rust allows -----------------
        if c in "rbc\"":
            start = i
            j = i
            prefix_ok = (i == 0 or text[i - 1] not in IDENT_CHARS)
            if prefix_ok:
                if text[j] in "bc" and j + 1 < n and text[j + 1] in "r\"":
                    j += 1
                raw = False
                if j < n and text[j] == "r":
                    raw = True
                    j += 1
                hashes = 0
                while raw and j < n and text[j] == "#":
                    hashes += 1
                    j += 1
                if j < n and text[j] == '"':
                    if raw:
                        term = '"' + "#" * hashes
                        end = text.find(term, j + 1)
                        if end == -1:
                            fail_at = start
                            # Unterminated raw string: not compilable Rust. Blank to EOF
                            # rather than guess, so no structure is invented after it.
                            for k in range(fail_at, n):
                                code[k] = STR_SENTINEL
                            strings[fail_at] = text[j + 1:n]
                            i = n
                            continue
                        content = text[j + 1:end]
                        stop = end + len(term)
                    else:
                        k = j + 1
                        buf = []
                        while k < n:
                            if text[k] == "\\" and k + 1 < n:
                                buf.append(text[k + 1] if text[k + 1] != "n" else "\n")
                                k += 2
                                continue
                            if text[k] == '"':
                                break
                            buf.append(text[k])
                            k += 1
                        content = "".join(buf)
                        stop = min(k + 1, n)
                    for k in range(start, stop):
                        code[k] = STR_SENTINEL
                    strings[start] = content
                    i = stop
                    continue
        # --- char literals ---------------------------------------------------
        if c == "'":
            if i + 1 < n and text[i + 1] == "\\":
                k = i + 2
                while k < n and text[k] not in ("'", "\n"):
                    k += 1
                if k < n and text[k] == "'":
                    for m2 in range(i, k + 1):
                        code[m2] = " "
                    i = k + 1
                    continue
            elif i + 2 < n and text[i + 2] == "'":
                for m2 in range(i, i + 3):
                    code[m2] = " "
                i += 3
                continue
            code[i] = " "   # a lifetime/label quote is inert
            i += 1
            continue
        i += 1
    return "".join(code), strings


# ---------------------------------------------------------------------------
# ANCHORED cfg HEADS ONLY (job 52 finding 1).
#
# A bare `cfg(` token is not an effect: `#[cfg_attr(docsrs, doc(cfg(feature = "x")))]`
# is DOCUMENTATION, and crediting the `cfg(` nested inside its `doc(...)` credits a
# feature that gates no compilation. So only these four heads count, and each is
# ANCHORED to the syntax that makes it a real gate:
#
#   #[cfg( … )]        #![cfg( … )]        cfg!( … )
#   #[cfg_attr( COND , … )]    #![cfg_attr( COND , … )]   — the CONDITION only
#
# A `cfg(...)` anywhere else — inside `doc(...)`, inside another attribute's token
# tree, inside a `cfg_attr` TAIL — is deliberately not a site.
# ---------------------------------------------------------------------------
WS = "[ \t\r\n]*"
HEAD_RE = re.compile(
    r'(?P<attr>\#!?\[' + WS + r'(?P<kind>cfg_attr|cfg)' + WS + r'\()'
    r'|(?P<bang>(?<![A-Za-z0-9_])cfg' + WS + r'!' + WS + r'\()'
)
FEATURE_EQ_RE = re.compile(r'(?<![A-Za-z0-9_])feature' + WS + r'=' + WS)


def balanced_span(code, open_idx):
    """open_idx points at '('. Return (start, end) of the CONTENT, or None if unbalanced.

    No string handling needed: string literals are sentinel-filled in `code`.
    """
    depth = 0
    i = open_idx
    n = len(code)
    while i < n:
        c = code[i]
        if c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
            if depth == 0:
                return (open_idx + 1, i)
        i += 1
    return None


def first_top_level_arg(code, start, end):
    """The first comma-separated argument of a predicate span, at depth 0."""
    depth = 0
    i = start
    while i < end:
        c = code[i]
        if c in "([{":
            depth += 1
        elif c in ")]}":
            depth -= 1
        elif c == "," and depth == 0:
            return (start, i)
        i += 1
    return (start, end)


def cfg_feature_sites(code, strings):
    """Yield (feature_name, offset) for every feature named in a real cfg predicate."""
    for m in HEAD_RE.finditer(code):
        kind = m.group("kind") if m.group("attr") else "cfg"
        span = balanced_span(code, m.end() - 1)
        if span is None:
            # Unbalanced: not compilable Rust. Left to rustc rather than guessed at.
            continue
        start, end = span
        if kind == "cfg_attr":
            start, end = first_top_level_arg(code, start, end)
        for fm in FEATURE_EQ_RE.finditer(code, start, end):
            name = strings.get(fm.end())
            if name:
                yield name, fm.start()


# ---------------------------------------------------------------------------
# BUILD-SCRIPT ENV READS (job 52 finding 3).
#
# cargo sets CARGO_FEATURE_<NAME> in a build script's environment, so reading one IS a
# cfg-equivalent effect — but only in a BUILD SCRIPT, and only through a genuine
# environment API. A bare `var("CARGO_FEATURE_X")` is not one: `var` is an ordinary
# identifier, and an unrelated local function of that name would otherwise credit a
# dead feature. So `var`/`var_os` unqualified is accepted ONLY when the file proves the
# import (`use std::env::var`, including a braced list). `option_env!` is deliberately
# NOT accepted: cargo documents these variables for build-script EXECUTION, and
# claiming a compile-time read would be asserting something unverified.
# ---------------------------------------------------------------------------
# THREE spellings, each ANCHORED (roborev job 55, finding 2). An unanchored
# `env::var(` accepted a LOCAL `mod env` — an ordinary module of that name reading
# nothing from the process environment — and, having no left anchor at all, it also
# matched the `env::var` SUFFIX of `my_env::var(...)`. Both credited a dead feature.
#
#   ENV_ABS_RE  — a full `std::env::var` / `core::env::var` path (a leading `::` is
#                 allowed), anchored so `my_std::env::var` and `crate::std::env::var`
#                 do NOT match.
#   ENV_MOD_RE  — a bare `env::var`, accepted ONLY when the file proves the module
#                 binding (`use std::env;`, including a braced `use std::{env, fs};`),
#                 and anchored so `my_env::var` does not match.
#   ENV_FN_RE   — a bare `var`/`var_os`, accepted ONLY when the file proves the
#                 function binding (`use std::env::var`, braced lists included).
PATH_SEG = r'(?<![A-Za-z0-9_:])(?:::)?'
ENV_ABS_RE = re.compile(PATH_SEG + r'(?:std|core)' + WS + r'::' + WS + r'env' + WS + r'::' + WS + r'(?P<fn>var_os|var)' + WS + r'\(' + WS)
ENV_MOD_RE = re.compile(r'(?<![A-Za-z0-9_:])env' + WS + r'::' + WS + r'(?P<fn>var_os|var)' + WS + r'\(' + WS)
ENV_FN_RE = re.compile(r'(?<![A-Za-z0-9_:])(?P<fn>var_os|var)' + WS + r'\(' + WS)
USE_ENV_FN_RE = re.compile(r'(?<![A-Za-z0-9_])use[ \t\r\n]+(?:::)?(?:std|core)' + WS + r'::' + WS + r'env' + WS + r'::' + WS + r'(?P<tail>\{[^}]*\}|var_os|var)')
USE_ENV_MOD_RE = re.compile(r'(?<![A-Za-z0-9_])use[ \t\r\n]+(?:::)?(?:std|core)' + WS + r'::' + WS + r'(?P<tail>env' + WS + r'[;,}]|\{[^}]*\})')
CARGO_FEATURE_RE = re.compile(r'^CARGO_FEATURE_([A-Z0-9_]+)$')


def imported_env_fns(code):
    """The `std::env` function names this file has imported by name."""
    names = set()
    for m in USE_ENV_FN_RE.finditer(code):
        tail = m.group("tail")
        if tail.startswith("{"):
            for part in tail[1:-1].split(","):
                part = part.strip()
                if part in ("var", "var_os"):
                    names.add(part)
        else:
            names.add(tail)
    return names


def imports_env_module(code):
    """True when this file binds the `std::env` MODULE (so a bare `env::var` is std's)."""
    for m in USE_ENV_MOD_RE.finditer(code):
        tail = m.group("tail").strip()
        if tail.startswith("{"):
            for part in tail[1:-1].split(","):
                if part.strip() == "env":
                    return True
        else:
            return True
    return False


def build_script_env_features(code, strings):
    """Yield (CARGO_FEATURE_<X> suffix, offset) for every genuine env read."""
    imported_fns = imported_env_fns(code)
    mod_bound = imports_env_module(code)
    seen = set()

    def emit(m):
        name = strings.get(m.end())
        if not name:
            return None
        hit = CARGO_FEATURE_RE.match(name)
        if not hit or m.start() in seen:
            return None
        seen.add(m.start())
        return hit.group(1), m.start()

    for m in ENV_ABS_RE.finditer(code):
        got = emit(m)
        if got:
            yield got
    if mod_bound:
        for m in ENV_MOD_RE.finditer(code):
            got = emit(m)
            if got:
                yield got
    for m in ENV_FN_RE.finditer(code):
        if m.group("fn") not in imported_fns:
            continue
        got = emit(m)
        if got:
            yield got


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
# 2) DEPENDENCY KEYS, with what each declaration ALREADY ENABLES.
#
#    Cargo's feature syntax names a dependency by its KEY — the `rename` when the
#    manifest renames it (`bee = { package = "b" }` is written `bee/bfeat`, never
#    `b/bfeat`) — so resolving `pkg/feature` edges by PACKAGE NAME misses a renamed
#    workspace member, classifies it as EXTERNAL and auto-credits the edge without ever
#    checking the forwarded feature: a false PASS.
#
#    Each declaration's `features` and `uses_default_features` are kept too (job 52
#    finding 2), because an edge that enables what the DECLARATION already enables
#    changes nothing: `foo = ["serde/derive"]` with `serde = { features = ["derive"] }`
#    is a no-op, and so is `foo = ["dep/default"]` on a dependency that already uses
#    default features. Workspace-dependency inheritance is already resolved in cargo
#    metadata, so an inherited `features = [...]` is visible here.
#
#    Redundancy is judged over ALL declarations under the key (normal, dev, build,
#    per-target): credit is withheld only when EVERY one of them already enables the
#    feature, which is the direction that cannot invent a false FAIL.
# ---------------------------------------------------------------------------
for name, rec in members.items():
    keys = {}
    for dep in rec["dependencies"]:
        dname = dep.get("name")
        if not dname:
            fail("member '%s' has a dependency with no name in cargo metadata. Refusing to resolve feature edges against an unreadable dependency table." % name)
        key = dep.get("rename") or dname
        is_local = dep.get("path") is not None or dep.get("source") is None
        dfeats = dep.get("features")
        if dfeats is None:
            dfeats = []
        if not isinstance(dfeats, list):
            fail("member '%s' dependency '%s' has a non-list `features` in cargo metadata. Refusing to guess at its shape." % (name, key))
        entry = keys.setdefault(key, {"package": dname, "member": None, "optional": False, "decls": []})
        if entry["package"] != dname:
            fail("member '%s' uses the dependency key '%s' for two different packages ('%s' and '%s') in cargo metadata. Refusing to resolve feature edges through an ambiguous key." % (name, key, entry["package"], dname))
        if dep.get("optional"):
            entry["optional"] = True
        entry["decls"].append({
            "features": set(dfeats),
            "default": bool(dep.get("uses_default_features", True)),
        })
        if is_local and dname in members:
            entry["member"] = dname
    rec["dep_keys"] = keys


def edge_is_redundant(info, dep_feature):
    """True when EVERY declaration under this key already enables dep_feature."""
    decls = info["decls"]
    if not decls:
        return False
    for d in decls:
        if dep_feature in d["features"]:
            continue
        if dep_feature == "default" and d["default"]:
            continue
        return False
    return True


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
#    Two registrations per target: its EXACT src_path, which always wins, and the TREE
#    under `dirname(src_path)`, which is how a target reaches its module files —
#    EXCEPT for a custom-build target, whose dirname is the PACKAGE ROOT. Registering
#    that would hand a package the whole of its own directory again, nested members and
#    all, so a build script is registered as an exact file only.
#
#    Nested-member exclusion: a file inside a DEEPER member's package directory is not
#    the shallower member's source, unless it is literally one of its target files.
# ---------------------------------------------------------------------------
TREELESS_KINDS = {"custom-build"}
exact_owners = {}
tree_owners = []
buildscript_files = {}

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
                continue
            owners.add(name)
    return owners


# ---------------------------------------------------------------------------
# 4) E1 — reference sites, per OWNING package, over the LEXED text.
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
        code, strings = lex(text)
        rel = os.path.relpath(full, REPO_ROOT)
        sites = list(cfg_feature_sites(code, strings))
        env_sites = []
        if full in buildscript_files:
            env_sites = list(build_script_env_features(code, strings))
        for owner in owners:
            record = members[owner]["refsites"]
            for feat, off in sites:
                if feat not in record:
                    record[feat] = "%s:%d" % (rel, code.count("\n", 0, off) + 1)
            if not env_sites or owner not in buildscript_files.get(full, ()):
                continue
            for env, off in env_sites:
                for feat in members[owner]["features"]:
                    if cargo_feature_env_name(feat) == env and feat not in record:
                        record[feat] = "%s:%d (CARGO_FEATURE_%s)" % (rel, code.count("\n", 0, off) + 1, env)

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
#    it unless that optional dependency is activated by something else. The `?` is
#    preserved here and the edge is evaluated per ORIGIN in step 7, live only when the
#    dependency is activated somewhere in that origin's closure.
#
#    A REDUNDANT edge is not an effect either (job 52 finding 2): if the dependency
#    declaration already enables the feature, enabling it again changes nothing, so the
#    edge is dropped entirely — no credit and no closure edge.
#
#    A NON-weak `dep/feature` on an OPTIONAL dependency also ACTIVATES it (cargo's
#    documented behaviour), so it is both an effect and an activation.
# ---------------------------------------------------------------------------
uncond = {}
edges = {}
own_deps = {}
ext_edges = {}
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
                # ACTIVATION IS JUDGED FIRST, and that ORDER is the fix for a false
                # FAIL (roborev job 55, finding 1): a non-weak edge to an OPTIONAL
                # dependency ACTIVATES that dependency, which is an effect no matter
                # what the declaration already enables. `f = ["foo/x"]` beside
                # `foo = { optional = true, features = ["x"] }` genuinely pulls `foo`
                # into the build, so dropping it as redundant reported a LIVE feature
                # as dead — and a guard that reds on correct input is the guard agents
                # learn to waive.
                activates = (not weak) and info["optional"]
                if activates:
                    acts.add(key)
                    effect = True
                if edge_is_redundant(info, dfeat) and not activates:
                    # The dependency declaration already enables this feature and the
                    # edge activates nothing, so it newly enables NOTHING: not an
                    # effect, and not a closure edge.
                    continue
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
    Nor does a WEAK `dep?/feature` edge whose optional dependency nothing activates,
    nor a REDUNDANT `dep/feature` edge the dependency declaration already enables.

    A cfg site must be a REAL cfg: `#[cfg(...)]`, `#![cfg(...)]`, `cfg!(...)` or the
    CONDITION of `#[cfg_attr(...)]`. A feature named in a string literal, in doc text,
    or in a `doc(cfg(...))` inside a `cfg_attr` tail gates no compilation.

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
# the target sources and computing every closure — and it ends by DECLARING the
# residual of its own method, because a lane that omits coverage silently is
# indistinguishable from one that covers it. The `features-load-bearing` component of
# scripts/agent-gate.sh matches this line WHOLE.
print(
    "features-load-bearing: %d/%d declared features load-bearing across %d workspace manifests "
    "(%d exempt: %s); %d Rust source files scanned for reference sites; %s"
    % (asserted, asserted, len(members), exempt_count, ", ".join(sorted(EXEMPT_FEATURES)), scanned_files, RESIDUAL_NOTE)
)
PYEOF

if ! python3 "$READER" "$REPO_ROOT" "$METADATA"; then
  exit 1
fi
