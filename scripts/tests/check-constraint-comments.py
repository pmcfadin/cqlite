#!/usr/bin/env python3
"""A comment that states a CONSTRAINT must sit beside code that ENFORCES it.

WHY THIS EXISTS (issue #3234, roborev round 12)
-----------------------------------------------
Three findings on one issue were the same defect, not three:

* L3 — the manifest carried 11 fields claiming properties nothing had observed;
* L4 — the committed manifest documented a contract the writer no longer emitted;
* F2 — ``validated_sstable_dir`` documented ``<table>-<uuid>`` and accepted
  ``<table>-*``, so ``<table>-backup`` passed the check its own comment described.

Each time, prose asserted a constraint the adjacent code did not enforce, and each
time it took a human reviewer to notice. That is a habit of the change, so it is
mechanized here: for a small, named set of SHAPE CLAIMS, a comment making the claim
must either NAME its enforcement or be followed, within a stated window of real code,
by an enforcement of it. Otherwise the comment goes — an unenforceable claim must be
ABSENT, not softened.

SCOPE, stated precisely so it is checkable
------------------------------------------
* Files: the ``SURFACE`` list below — the #3234 PRODUCTION surface (the corpus
  generator, its manifest writer, its row driver, and the AC3 harness modules). Test
  suites are deliberately out of scope: their comments describe what their CASES
  assert, and the cases plus the declared case-count floor already mechanize that.
* Claims: the ``CLAIMS`` table below. Each is a shape/format/cardinality claim that
  has a machine-checkable enforcement token. This is NOT a general English-to-code
  verifier and does not pretend to be: a narrow guard that genuinely fires beats a
  broad one that cannot.
* Satisfied when the enforcement token appears EITHER inside the claiming comment
  block (a claim that names the code enforcing it is traceable) OR within
  ``WINDOW_LINES`` of non-comment code after the block. Enforcement found only inside
  ANOTHER comment does not count — a comment cannot enforce a comment.

Exit 0 = no unenforced claims. Exit 1 = at least one, named with file:line and the
enforcement it lacks. Exit 2 = usage/IO error.

``--self-test`` runs BOTH directions: the real surface must pass, and a deliberately
injected unenforced claim must FAIL. A guard nobody has watched fire is a claim, not a
guard — the same standard this issue applies to everything else.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
import tempfile

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

SURFACE = [
    "test-data/scripts/gen-perf-corpus-bti.sh",
    "test-data/scripts/write-perf-corpus-bti-manifest.py",
    "test-data/scripts/gen-perf-corpus-bti-rows.py",
    "cqlite-core/examples/bti_perf_scan/main.rs",
    "cqlite-core/examples/bti_perf_scan/manifest.rs",
    "cqlite-core/examples/bti_perf_scan/scope.rs",
]

WINDOW_LINES = 60

# (id, claim-in-comment, enforcement-in-code, what to do about it)
CLAIMS = [
    (
        "table-dir-shape",
        r"<table>-<uuid>|\$TBL-<uuid>|<table>-<32 hex>|\$TBL-<32 hex>|<table>-<id>|"
        r"\b32 hex digits\b",
        r"TABLE_ID_HEX_LEN|is_table_dir|\[0-9a-f\]\{32\}|\{32\}\$|is_ascii_hexdigit|"
        r"TABLE_UUID",
        "state the enforcement (name `is_table_dir` / the `[0-9a-f]{32}` test) or drop "
        "the shape from the comment — `<table>-*` is not `<table>-<32 hex>`",
    ),
    (
        "da-descriptor",
        r"da-<gen>-bti|da-\*-bti",
        r"DESCRIPTOR_RE|GENERATION_RE|\^da-|da-\*-bti|da-\\\*-bti",
        "name the descriptor check (`DESCRIPTOR_RE`, the `da-*-bti-*` glob assert) or "
        "drop the descriptor shape from the comment",
    ),
    (
        "absolute-path",
        r"must be an absolute|must be absolute|ABSOLUTE path|absolute path",
        r"== /\*|os\.path\.isabs|starts_with\('/'\)|pwd -P|realpath|abspath",
        "enforce absoluteness beside the claim (`[[ $x == /* ]]`, `os.path.isabs`) or "
        "drop it",
    ),
    (
        "exactly-one",
        r"exactly ONE|exactly one `|exactly one <",
        r"-eq 1\b|== 1\b|!= 1\b|\.len\(\) == 1|len\(\w+\) == 1|\[0\]|-gt 1\b",
        "enforce the cardinality beside the claim (a `-eq 1` / `len(...) == 1` refusal) "
        "or drop it",
    ),
]

COMMENT_PREFIX = {".sh": ("#",), ".py": ("#",), ".rs": ("//", "//!", "///")}


def comment_prefixes(path: str) -> tuple[str, ...]:
    for ext, pre in COMMENT_PREFIX.items():
        if path.endswith(ext):
            return pre
    return ("#",)


def is_comment(line: str, prefixes: tuple[str, ...]) -> bool:
    s = line.strip()
    return any(s.startswith(p) for p in prefixes)


def blocks(lines: list[str], prefixes: tuple[str, ...]):
    """Consecutive comment lines, as (start_line_no, end_index, text)."""
    i = 0
    while i < len(lines):
        if is_comment(lines[i], prefixes):
            j = i
            while j < len(lines) and is_comment(lines[j], prefixes):
                j += 1
            yield (i + 1, j, "\n".join(lines[i:j]))
            i = j
        else:
            i += 1


def code_window(lines: list[str], start: int, prefixes: tuple[str, ...]) -> str:
    """The next WINDOW_LINES lines of NON-COMMENT code after `start` (0-based)."""
    out = []
    for line in lines[start : start + WINDOW_LINES]:
        if not is_comment(line, prefixes):
            out.append(line)
    return "\n".join(out)


def check_file(path: str) -> list[str]:
    abs_path = path if os.path.isabs(path) else os.path.join(REPO_ROOT, path)
    with open(abs_path, encoding="utf-8") as fh:
        lines = fh.read().splitlines()
    prefixes = comment_prefixes(path)
    problems = []
    for start_no, end_idx, text in blocks(lines, prefixes):
        window = code_window(lines, end_idx, prefixes)
        for claim_id, claim_re, enforce_re, remedy in CLAIMS:
            m = re.search(claim_re, text)
            if not m:
                continue
            if re.search(enforce_re, text) or re.search(enforce_re, window):
                continue
            problems.append(
                f"{path}:{start_no}: comment claims `{claim_id}` "
                f"({m.group(0)!r}) but no enforcement of it appears in the comment or "
                f"in the next {WINDOW_LINES} lines of code.\n"
                f"    remedy: {remedy}"
            )
    return problems


def run(files: list[str]) -> int:
    problems: list[str] = []
    for f in files:
        try:
            problems.extend(check_file(f))
        except OSError as e:
            print(f"check-constraint-comments: cannot read {f}: {e}", file=sys.stderr)
            return 2
    if problems:
        print("UNENFORCED CONSTRAINT COMMENT(S) — a comment that states a constraint "
              "must name or sit beside its enforcement, or the comment goes (#3234):")
        for p in problems:
            print(f"  {p}")
        return 1
    print(f"CONSTRAINT-COMMENTS-OK ({len(files)} file(s), {len(CLAIMS)} claim kinds, "
          f"window {WINDOW_LINES} lines)")
    return 0


def self_test() -> int:
    """Both directions: the surface passes, and an injected claim FAILS."""
    failures = []
    if run(SURFACE) != 0:
        failures.append("the real #3234 surface has an unenforced constraint comment")
    else:
        print("ok   - positive control: the real surface passes")

    with tempfile.TemporaryDirectory() as tmp:
        # A copy of the surface file whose OWN F2 finding this guard exists for, with an
        # unenforced claim appended where no enforcement can follow it.
        src = os.path.join(REPO_ROOT, "cqlite-core/examples/bti_perf_scan/manifest.rs")
        dst = os.path.join(tmp, "injected.rs")
        shutil.copy(src, dst)
        with open(dst, "a", encoding="utf-8") as fh:
            fh.write(
                "\n// The sstable_dir is required to be `<table>-<uuid>`, exactly 32 hex\n"
                "// digits, and nothing here checks it.\n"
            )
        rc = subprocess.run(
            [sys.executable, os.path.abspath(__file__), "--files", dst],
            capture_output=True, text=True,
        )
        if rc.returncode == 1 and "table-dir-shape" in rc.stdout:
            print("ok   - negative control: an injected unenforced claim FAILS the guard")
        else:
            failures.append(
                "an injected unenforced claim did NOT fail the guard "
                f"(rc={rc.returncode}, out={rc.stdout.strip()[:200]!r})"
            )

        # ...and the SAME claim passes once its enforcement sits beside it, so the guard
        # is not simply refusing the words.
        dst2 = os.path.join(tmp, "enforced.rs")
        shutil.copy(src, dst2)
        with open(dst2, "a", encoding="utf-8") as fh:
            fh.write(
                "\n// The sstable_dir is required to be `<table>-<uuid>`, exactly 32 hex\n"
                "// digits.\n"
                "fn injected(name: &str, table: &str) -> bool {\n"
                "    crate::scope::is_table_dir(name, table)\n"
                "}\n"
            )
        rc2 = subprocess.run(
            [sys.executable, os.path.abspath(__file__), "--files", dst2],
            capture_output=True, text=True,
        )
        if rc2.returncode == 0:
            print("ok   - control: the same claim PASSES when its enforcement follows it")
        else:
            failures.append(
                "an ENFORCED claim was reported as unenforced — the guard is refusing "
                f"words, not checking code (out={rc2.stdout.strip()[:200]!r})"
            )

    if failures:
        for f in failures:
            print(f"FAIL - {f}")
        return 1
    print("check-constraint-comments: SELF-TEST ALL PASS (3 cases)")
    return 0


def main(argv: list[str]) -> int:
    args = argv[1:]
    if args and args[0] == "--self-test":
        return self_test()
    if args and args[0] == "--files":
        if len(args) < 2:
            print("--files requires at least one path", file=sys.stderr)
            return 2
        return run(args[1:])
    if args:
        print(__doc__)
        return 2
    return run(SURFACE)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
