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
#       THE BUILD-SCRIPT ROUTE is deliberately MAXIMAL, and this is where the contract
#       shows its teeth. Recognising an env READ was narrowed three times (an anchored
#       API path, a proven module binding, a proven function binding) and review found
#       another spelling each time — a constant argument, `env::vars()` iteration, an
#       aliased `use std::env::var as get_var`, a local wrapper — every one of which
#       reported a LIVE feature DEAD. So: in a package that HAS a build script, a
#       feature is credited when `CARGO_FEATURE_<NAME>` appears IN ANY FORM ANYWHERE in
#       that package's `.rs` sources, comments and strings included. A BARE
#       `CARGO_FEATURE_` prefix (environment iteration names no individual feature)
#       credits EVERY feature of that package. A package with NO build script gets no
#       env credit at all — cargo sets these variables for a build script's execution,
#       so without one there is nothing to read them.
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
# `fuzz/` — the one at the WORKSPACE ROOT — is its own EXCLUDED workspace (CLAUDE.md:
# the gate and default builds never compile it), so it is out of this guard's scope: its
# features are neither certified nor reported here. That exclusion is by exact PATH, not
# by directory name: a `fuzz` (or `target`) module inside a package's `src/` is ordinary
# Rust source and IS scanned.
#
# # PREREQUISITES: cargo AND python3, BOTH MANDATORY
#
# `cargo metadata` is the only source of truth for the feature set (see above), and the
# reader that parses its JSON and lexes Rust is python3. Neither is optional and there is
# no fallback derivation for either: this guard FAILS with a named remedy when one is
# missing, and NEVER SKIPs, because a verdict it did not measure is not a verdict. A
# POSIX-tool reimplementation was considered and rejected — a second implementation of a
# JSON parser and a Rust lexer is a second thing to get wrong, and its correctness would
# only be knowable by differential testing against the first.
#
# # THE CONTRACT: A SCOPED NO-FALSE-FAIL CLAIM, AND EXPLICIT INCOMPLETENESS
#
# This is a LEXICAL scan over Rust, not a compiler, so it cannot be both complete and
# sound. It chooses to avoid FALSE FAILS — but the claim is SCOPED, and the scope is the
# whole point:
#
#   * NO FALSE FAIL for a gate written in a RECOGNISED spelling. The recognised set is
#     enumerated in the printed contract line: `#[cfg]`, `#![cfg]`, `cfg!`, and
#     `cfg_attr` (its condition AND a `cfg`/`cfg_attr` in its tail), with whitespace
#     tolerated between `#`, `!` and `[`, and Rust string escapes decoded. Within that
#     set every ambiguity — ownership, an undecodable escape, a build-script env key —
#     resolves toward CREDITING.
#   * A gate OUTSIDE that set is NOT SEEN, and such a feature would be reported DEAD.
#     Two cases are known and neither is lexically resolvable: a cfg whose feature NAME
#     is produced by MACRO EXPANSION, and a build-script env key CONSTRUCTED AT RUNTIME
#     (a name joined from fragments).
#   * INCOMPLETE — a DEAD feature can escape. False PASSes are permitted and DECLARED.
#
# WHY THE CLAIM IS SCOPED RATHER THAN ABSOLUTE. It was written unqualified
# ("SOUND-BY-DESIGN: a live feature is not reported dead") and six consecutive review
# rounds produced six more valid spellings it did not recognise — `# [cfg(...)]` with
# whitespace, a `cfg_attr` tail, an `\x66` escape, an aliased env import, an
# out-of-tree `#[path]` module, a nested-member helper. Rust admits unboundedly many, so
# the absolute claim can never be made true, and chasing it is the recogniser treadmill
# this guard already escaped for false PASSes — recreated one level up. An unenforceable
# claim gets NARROWED to what is enforceable, never dropped whole and never left
# overstated: a false rationale printed in a gate log is worse than none, because it is
# what stops the next person looking.
#
# IMPLEMENTING more of Rust is deliberately out of bounds: scope-aware name resolution,
# module-graph traversal and macro expansion in bash are the unbounded-parsing problem
# this repo has already paid for and REMOVED a guard over (#1712 deleted `pub-surface`'s
# rustdoc/public-API half precisely because a scanner that must find declarations
# anywhere in arbitrary source cannot abstain).
#
# THE DECLARED ESCAPE ROUTES (each pinned by a fixture in
# scripts/tests/test_features_load_bearing_guard.sh, because a declaration nobody tests
# is a comment that rots):
#
#   * cfgs inside an UNEXPANDED MACRO BODY — a `#[cfg(feature = "x")]` in a
#     `macro_rules!` body is read as tokens, not expanded, so it credits `x` even where
#     no expansion applies it in the declaring package.
#   * ORPHAN `.rs` files under a target's source directory — one no `mod` chain reaches
#     from the target root is scanned as if compiled, so a cfg gate in dead code credits
#     its feature. Deciding it means resolving the module graph.
#   * ANY TEXTUAL `CARGO_FEATURE_*` MENTION in a build-script package's sources credits
#     that feature — no API, module or scope analysis, comments and strings included —
#     and a bare `CARGO_FEATURE_` prefix credits every feature of that package. Three
#     narrower rules each reported a live feature dead (see E1); this one cannot.
#   * AN UNDECODABLE STRING ESCAPE in a cfg predicate credits EVERY feature of the
#     package, because recording a wrong name would report the real feature dead.
#   * A `.rs` FILE WITH NO UNAMBIGUOUS OWNER credits EVERY candidate package — every
#     member whose package directory contains it — and a file beneath a NESTED member's
#     directory credits the OUTER member too. A module included from outside any target
#     root (`#[path = "../gated.rs"]`, `include!`) is reachable by a target this scan
#     cannot trace, and subtracting an owner reported a feature used only there as dead.
#   * INDIRECTLY redundant dependency-feature edges — `dep/x` where the declaration
#     enables some feature that itself enables `x`. Deciding it needs the dependency's
#     own feature table, which `--no-deps` does not carry for external crates.
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
  || fail "python3 is not on PATH. It is a MANDATORY prerequisite of this guard, not an optional accelerator: the metadata reader parses cargo's JSON and lexes Rust, and there is no POSIX-tool fallback (a second implementation of either would be a second thing to get wrong). Remedy: install python3 (>=3.6). This guard never SKIPs — a verdict it did not measure is not a verdict."

WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/features-load-bearing.XXXXXX")"
cleanup() { rm -rf "$WORK_DIR"; return 0; }
trap cleanup EXIT

METADATA="$WORK_DIR/metadata.json"
# --no-deps: only workspace members are certified.
# --locked: this component is MANDATORY on the gate of record, and a `cargo metadata`
# that resolves dependencies can WRITE Cargo.lock — which would mutate the checkout
# mid-run and trip the gate's own tree-integrity check (#2926,
# `tree-mutated-midrun`), presenting as a mystery failure with no obvious cause. With
# --locked a stale lockfile is a NAMED failure instead of a silent write. Measured: with
# --no-deps this needs no lockfile at all (a fresh workspace with none resolves fine and
# none is written), so --locked costs nothing and cannot red a legitimate tree.
if ! (cd "$REPO_ROOT" && cargo metadata --no-deps --locked --format-version 1) >"$METADATA" 2>"$WORK_DIR/metadata.err"; then
  echo "" >&2
  sed -n '1,40p' "$WORK_DIR/metadata.err" >&2
  fail "\`cargo metadata --no-deps --locked\` FAILED (see above), so the declared-feature set could not be derived. Fail-closed: an unmeasured feature set is never an empty one. Remedy: fix the manifest error cargo reports, or — if it says the lockfile needs updating — run \`cargo metadata >/dev/null\` and commit the Cargo.lock change. --locked is deliberate: this guard must not silently rewrite the lockfile mid-gate (#2926 tree-integrity)."
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

# DIRECTORY PRUNING IS BY ANCHORED PATH, NOT BASENAME (roborev job 62). `src/target/`
# and `src/fuzz/` are legitimate Rust module directories, and pruning every directory
# that happens to share those names meant a gate written there — in a RECOGNISED
# spelling — was never scanned and its feature reported dead, contradicting the very
# claim this guard prints.
#
#   `.git`, `node_modules`  — pruned by name at any depth: neither is ever a Rust
#                             module directory, at any level, so the name IS the
#                             property.
#   `target`                — pruned ONLY beside a `Cargo.toml`, which is where cargo
#                             puts build output. `src/target/` has no sibling manifest
#                             and is scanned.
#   `<root>/fuzz`           — pruned as EXACTLY that one path: it is the excluded
#                             cargo-fuzz workspace (see the header). `src/fuzz/`, or a
#                             `fuzz` module anywhere else, is scanned.
PRUNE_BY_NAME = {".git", "node_modules"}
FUZZ_WORKSPACE = os.path.join(REPO_ROOT, "fuzz")


def prune_dir(dirpath, name):
    full = os.path.join(dirpath, name)
    if name in PRUNE_BY_NAME:
        return True
    if name == "target" and os.path.isfile(os.path.join(dirpath, "Cargo.toml")):
        return True
    if os.path.realpath(full) == os.path.realpath(FUZZ_WORKSPACE):
        return True
    return False

# THE CONTRACT, printed on every success — SCOPED, because the unqualified version could
# not be made true (roborev job 60). It read "SOUND-BY-DESIGN: a live feature is not
# reported dead", an ABSOLUTE claim about a lexical scan of Rust — and Rust has
# unboundedly many valid spellings of a gate, so six review rounds produced six more
# witnesses and would have produced a seventh. That is the recogniser treadmill this
# guard already escaped for false PASSes, recreated one level up by a universal claim.
#
# So the claim is bounded by what the scanner RECOGNISES, and the recognised set is
# enumerated in the line itself: no false FAIL for a gate written in one of those
# spellings; a gate written outside that set is NOT SEEN, and the two known such cases
# are named. Within the recognised set every ambiguity still resolves toward CREDITING.
# The INCOMPLETE half is unchanged: a dead feature can escape, and the routes are listed.
#
# This repo's rule is that an unenforceable claim gets narrowed to what IS enforceable,
# never dropped whole and never left overstated — a scoped claim that holds beats a
# universal one that does not.
CONTRACT_LINE = (
    "CONTRACT: NO FALSE FAIL for a gate written in a RECOGNISED spelling — #[cfg], "
    "#![cfg], cfg!, and cfg_attr (its condition AND a cfg/cfg_attr in its tail), with "
    "whitespace tolerated between # ! [ and Rust string escapes decoded — and INCOMPLETE "
    "(a dead feature can escape). A gate written in a spelling OUTSIDE that set is NOT "
    "SEEN; two such are known and are not lexically resolvable: a cfg whose feature NAME "
    "is produced by MACRO EXPANSION, and a build-script env key CONSTRUCTED AT RUNTIME. "
    "Escape routes: cfgs inside unexpanded macro bodies; orphan .rs files under a target "
    "source dir; any textual CARGO_FEATURE_* mention in a build-script package's sources "
    "(no API, module or scope analysis; a bare CARGO_FEATURE_ prefix credits every "
    "feature of that package); an undecodable string escape credits every feature of the "
    "package; a .rs file with no unambiguous owner credits EVERY candidate package, and "
    "one under a nested member's dir credits the outer member too; indirectly redundant "
    "dependency edges."
)

STR_SENTINEL = "\x01"   # every byte of a string literal, in the cleaned text
# A feature value this scanner could not read confidently: credits EVERY feature of the
# owning package (never one wrong name).
AMBIGUOUS_NAME = "\x02ambiguous\x02"
# The `cfg_attr` tail recursion is bounded (a cyclic token stream must not hang the
# gate), and AT the bound the ambiguity is CREDITED rather than dropped — see
# _cfg_span_features.
CFG_ATTR_MAX_DEPTH = 16
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
# STRING ESCAPES ARE DECODED, because the NAME matters: `#[cfg(feature = "\\x66oo")]`
# gates the feature `foo`, and recording `x66oo` would report `foo` DEAD (roborev job 60).
# An escape this decoder cannot read confidently makes the value AMBIGUOUS, and an
# ambiguous value credits EVERY declared feature of the package rather than one wrong
# name — ambiguity resolves toward crediting, as everywhere else here.
def decode_escapes(raw):
    HEX = "0123456789abcdefABCDEF"
    out = []
    i = 0
    n = len(raw)
    while i < n:
        c = raw[i]
        if c != "\\":
            out.append(c)
            i += 1
            continue
        if i + 1 >= n:
            return "".join(out), True
        e = raw[i + 1]
        simple = {"n": "\n", "r": "\r", "t": "\t", "0": "\0", "\\": "\\", '"': '"', "'": "'"}
        if e in simple:
            out.append(simple[e])
            i += 2
            continue
        if e == "\n":
            # Line continuation: the newline and the following whitespace vanish.
            i += 2
            while i < n and raw[i] in " \t\r\n":
                i += 1
            continue
        if e == "x":
            h = raw[i + 2:i + 4]
            if len(h) == 2 and all(ch in HEX for ch in h):
                out.append(chr(int(h, 16)))
                i += 4
                continue
            return "".join(out), True
        if e == "u" and i + 2 < n and raw[i + 2] == "{":
            close = raw.find("}", i + 3)
            digits = raw[i + 3:close] if close != -1 else ""
            cleaned = digits.replace("_", "")
            if close != -1 and cleaned and all(ch in HEX for ch in cleaned):
                try:
                    out.append(chr(int(cleaned, 16)))
                except ValueError:
                    return "".join(out), True
                i = close + 1
                continue
            return "".join(out), True
        return "".join(out), True
    return "".join(out), False


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
                            strings[fail_at] = (text[j + 1:n], False)
                            i = n
                            continue
                        content = text[j + 1:end]
                        ambiguous = False
                        stop = end + len(term)
                    else:
                        k = j + 1
                        while k < n:
                            if text[k] == "\\" and k + 1 < n:
                                k += 2
                                continue
                            if text[k] == '"':
                                break
                            k += 1
                        content, ambiguous = decode_escapes(text[j + 1:k])
                        stop = min(k + 1, n)
                    for k in range(start, stop):
                        code[k] = STR_SENTINEL
                    strings[start] = (content, ambiguous)
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
# Rust permits whitespace between `#`, an optional `!` and `[` (`# [cfg(...)]`,
# `# ! [cfg(...)]` are both valid), so the head is whitespace-tolerant at every joint
# (roborev job 60): requiring `#[` contiguous meant a legal gate was NOT SEEN and its
# feature reported dead.
HEAD_RE = re.compile(
    r'(?P<attr>\#' + WS + r'!?' + WS + r'\[' + WS + r'(?P<kind>cfg_attr|cfg)' + WS + r'\()'
    r'|(?P<bang>(?<![A-Za-z0-9_])cfg' + WS + r'!' + WS + r'\()'
)
# NO `^` ANCHOR: this pattern is used with `re.match(code, pos, endpos)`, which already
# anchors at `pos`, while `^` would anchor at the real start of the STRING (a documented
# Python behaviour) and so never match a tail argument mid-file.
NESTED_CFG_RE = re.compile(WS + r'(?P<kind>cfg_attr|cfg)' + WS + r'\(')
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


def top_level_args(code, start, end):
    """The comma-separated arguments of a span, split at depth 0."""
    args = []
    depth = 0
    i = start
    arg_start = start
    while i < end:
        c = code[i]
        if c in "([{":
            depth += 1
        elif c in ")]}":
            depth -= 1
        elif c == "," and depth == 0:
            args.append((arg_start, i))
            arg_start = i + 1
        i += 1
    args.append((arg_start, end))
    return args


def _predicate_features(code, strings, start, end):
    """Every feature named directly in a cfg predicate span."""
    for fm in FEATURE_EQ_RE.finditer(code, start, end):
        entry = strings.get(fm.end())
        if entry is None:
            continue
        value, ambiguous = entry
        if ambiguous:
            # An escape this scanner could not decode: credit EVERY feature rather than
            # record a wrong name (see decode_escapes).
            yield AMBIGUOUS_NAME, fm.start()
        elif value:
            yield value, fm.start()


def _cfg_span_features(code, strings, kind, start, end, depth=0):
    """Features gated by a `cfg`/`cfg_attr` span, recursing into a cfg_attr TAIL.

    A `cfg_attr` applies its TAIL attributes when its condition holds, so
    `#[cfg_attr(unix, cfg(feature = "x"))]` is a REAL gate on `x` (roborev job 60) —
    scanning only the condition reported such a feature dead. Only a DIRECT `cfg`/
    `cfg_attr` item of the tail counts: a `cfg(...)` nested inside `doc(...)` is
    documentation and stays excluded, which is what the depth-0 argument split gives.
    """
    if kind == "cfg":
        for got in _predicate_features(code, strings, start, end):
            yield got
        return
    args = top_level_args(code, start, end)
    if not args:
        return
    cond_start, cond_end = args[0]
    for got in _predicate_features(code, strings, cond_start, cond_end):
        yield got
    for a_start, a_end in args[1:]:
        m = NESTED_CFG_RE.match(code, a_start, a_end)
        if not m:
            continue
        if depth >= CFG_ATTR_MAX_DEPTH:
            # AT THE LIMIT, CREDIT — never drop (roborev job 62). A deeper valid chain is
            # inside the advertised recognised spelling, so dropping it would report a
            # live feature dead; crediting every feature of the package can only let a
            # dead one escape, which the contract permits and declares.
            yield AMBIGUOUS_NAME, m.start()
            continue
        inner = balanced_span(code, m.end() - 1)
        if inner is None:
            continue
        for got in _cfg_span_features(code, strings, m.group("kind"), inner[0], inner[1], depth + 1):
            yield got


def cfg_feature_sites(code, strings):
    """Yield (feature_name, offset) for every feature named in a real cfg gate."""
    for m in HEAD_RE.finditer(code):
        kind = m.group("kind") if m.group("attr") else "cfg"
        span = balanced_span(code, m.end() - 1)
        if span is None:
            # Unbalanced: not compilable Rust. Left to rustc rather than guessed at.
            continue
        for got in _cfg_span_features(code, strings, kind, span[0], span[1]):
            yield got


# ---------------------------------------------------------------------------
# BUILD-SCRIPT `CARGO_FEATURE_*` — A TEXTUAL FAMILY SCAN, DELIBERATELY MAXIMAL
# (roborev job 58, finding 1).
#
# cargo sets `CARGO_FEATURE_<NAME>` in a build script's environment, so reading one is a
# cfg-equivalent effect. Recognising the READ is where three rounds of review kept
# finding holes: a constant argument, `std::env::vars()` iteration, an aliased import
# (`use std::env::var as get_var`), a local wrapper, a helper module. Every miss reports
# a LIVE feature dead, which the CONTRACT forbids — and every fix was another spelling.
#
# So the class is closed rather than narrowed: in a package that HAS a build script, a
# feature is credited if `CARGO_FEATURE_<NAME>` appears IN ANY FORM, ANYWHERE in that
# package's `.rs` sources — comments and strings included, no API, module or scope
# analysis. Over-crediting can only let a dead feature escape, which the contract
# permits and the contract line declares.
#
# THE BARE PREFIX counts too, and for the same reason: code that iterates the
# environment (`k.starts_with("CARGO_FEATURE_")`) names no individual feature, so there
# is nothing to match. A `CARGO_FEATURE_` with no `[A-Z0-9_]` name after it therefore
# credits EVERY feature of that package — the only reading that cannot report a live
# feature dead.
CARGO_FEATURE_MENTION_RE = re.compile(r'CARGO_FEATURE_([A-Z0-9_]*)')


def build_script_feature_mentions(raw_text):
    """Yield (suffix, offset) for every CARGO_FEATURE_* mention; '' means the bare prefix."""
    for m in CARGO_FEATURE_MENTION_RE.finditer(raw_text):
        yield m.group(1), m.start()


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
DIR_TO_MEMBER = {rec["dir"]: name for name, rec in members.items()}

for name, rec in members.items():
    keys = {}
    for dep in rec["dependencies"]:
        dname = dep.get("name")
        if not dname:
            fail("member '%s' has a dependency with no name in cargo metadata. Refusing to resolve feature edges against an unreadable dependency table." % name)
        key = dep.get("rename") or dname
        # A dependency is THIS workspace's member only when its resolved PATH is that
        # member's package directory. Matching by PACKAGE NAME (roborev job 55/57) sends
        # a non-member path dependency that happens to share a member's name into the
        # WRONG feature table: the forwarded feature is then looked up in the member,
        # and a feature that exists only in the real dependency is reported as
        # non-existent — a refusal on correct input, i.e. a false FAIL. The path is
        # canonical and bounded, so this is resolved properly rather than declared.
        dep_path = dep.get("path")
        dep_member = DIR_TO_MEMBER.get(os.path.realpath(dep_path)) if dep_path else None
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
        if dep_member is not None:
            entry["member"] = dep_member
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
#    NOTHING IS SUBTRACTED (roborev job 58): the nested-member exclusion that used to
#    live here dropped the OUTER owner of a file beneath a nested member's directory, and
#    an outer target can reach such a file through `#[path]`, `include!` or a shared
#    helper — so a feature used only there was reported dead. A file no target tree
#    covers falls back to every member whose package DIRECTORY contains it, which is what
#    covers a module included from outside any target root.
# ---------------------------------------------------------------------------
TREELESS_KINDS = {"custom-build"}
exact_owners = {}
tree_owners = []
# Members that HAVE a build script. Its `CARGO_FEATURE_*` reads are looked for in EVERY
# .rs file of that member's package directory, not only in build.rs (roborev job 57):
# a build script reaches helper modules through `mod`, `#[path]` and `include!`, and
# resolving those means implementing Rust's module graph in bash — the unbounded parsing
# problem #1712 removed a guard over. Missing such a read reports a LIVE feature dead,
# which the contract forbids, while over-crediting can only let a dead feature escape,
# which the contract allows. So the over-permissive route is taken deliberately and is
# named in the CONTRACT line as `package-wide (not module-graph) build-script env
# scanning`.
buildscript_members = set()

for name, rec in members.items():
    for target in rec["targets"]:
        sp = target.get("src_path")
        kinds = target.get("kind") or []
        if not sp:
            fail("target '%s' of member '%s' has no src_path in cargo metadata. Refusing to derive source ownership from an unreadable target." % (target.get("name"), name))
        sp = os.path.realpath(sp)
        exact_owners.setdefault(sp, set()).add(name)
        if set(kinds) & TREELESS_KINDS:
            buildscript_members.add(name)
            continue
        tree_owners.append((os.path.dirname(sp), name))

tree_owners.sort(key=lambda t: len(t[0]), reverse=True)


def buildscript_owners_of(path):
    """Members WITH a build script whose package directory contains this file."""
    if not buildscript_members:
        return ()
    out = []
    for name in buildscript_members:
        d = members[name]["dir"]
        if path == d or path.startswith(d + os.sep):
            out.append(name)
    return out


def containment_candidates(path):
    """Members whose PACKAGE DIRECTORY contains this file."""
    return {name for name, rec in members.items()
            if path == rec["dir"] or path.startswith(rec["dir"] + os.sep)}


def owners_of(path):
    """Every candidate owner — AMBIGUITY RESOLVES TOWARD CREDITING (roborev job 58, F2).

    Three layers, and none of them SUBTRACTS:

      * the exact `src_path` of a target — unambiguous;
      * any target's source TREE that contains the file. The nested-member exclusion
        that used to sit here was REMOVED: a module beneath a nested member's package
        directory can still be reached by an OUTER target (`#[path]`, `include!`, a
        shared helper), and dropping the outer owner reported a feature used only there
        as DEAD. A file two targets can reach genuinely references both packages'
        features, so it credits BOTH;
      * failing both, every member whose PACKAGE DIRECTORY contains the file. That is
        what covers a module included from OUTSIDE any target root
        (`#[path = "../gated.rs"]`, `include!`), which no tree covers and which
        therefore had no owner at all — again a live feature reported dead.

    Only a file under no member's directory has no owner, and then there is nothing it
    could possibly be compiled into.
    """
    owners = set(exact_owners.get(path, ()))
    for tree_dir, name in tree_owners:
        if name in owners:
            continue
        if path == tree_dir or path.startswith(tree_dir + os.sep):
            owners.add(name)
    if not owners:
        owners = containment_candidates(path)
    return owners


# ---------------------------------------------------------------------------
# 4) E1 — reference sites, per OWNING package, over the LEXED text.
# ---------------------------------------------------------------------------
scanned_files = 0
for dirpath, dirnames, filenames in os.walk(REPO_ROOT):
    dirnames[:] = [d for d in dirnames if not prune_dir(dirpath, d)]
    for fname in filenames:
        if not fname.endswith(".rs"):
            continue
        full = os.path.realpath(os.path.join(dirpath, fname))
        owners = owners_of(full)
        bs_owners = buildscript_owners_of(full)
        if not owners and not bs_owners:
            # Not a source file of any workspace-member target and not inside a
            # build-script package: a non-member crate's source (the measurement
            # harnesses under docs/reports/**), or a stray .rs no target reaches.
            # Nothing to certify.
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
        for owner in owners:
            record = members[owner]["refsites"]
            for feat, off in sites:
                line_no = code.count("\n", 0, off) + 1
                if feat == AMBIGUOUS_NAME:
                    for declared in members[owner]["features"]:
                        record.setdefault(declared, "%s:%d (undecodable escape)" % (rel, line_no))
                    continue
                if feat not in record:
                    record[feat] = "%s:%d" % (rel, line_no)
        if bs_owners:
            # RAW text, not the lexed code: a mention in a comment or a string counts too
            # (see the note above — the class is closed, not narrowed).
            mentions = list(build_script_feature_mentions(text))
            for owner in bs_owners:
                record = members[owner]["refsites"]
                for env, off in mentions:
                    line_no = text.count("\n", 0, off) + 1
                    for feat in members[owner]["features"]:
                        if feat in record:
                            continue
                        if env == "" or cargo_feature_env_name(feat) == env:
                            record[feat] = "%s:%d (CARGO_FEATURE_%s)" % (rel, line_no, env or "<bare prefix>")

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
    "(%d exempt: %s); %d Rust source files scanned for reference sites"
    % (asserted, asserted, len(members), exempt_count, ", ".join(sorted(EXEMPT_FEATURES)), scanned_files)
)
print("features-load-bearing: " + CONTRACT_LINE)
PYEOF

if ! python3 "$READER" "$REPO_ROOT" "$METADATA"; then
  exit 1
fi
