#!/usr/bin/env python3
"""Assert a WS0 `results.json` carries NONE of the DELETED interleaving verdict fields.

Usage: ws0_assert_no_verdict_fields.py <results.json> [more.json …]
Exit 0 when every document is clean; non-zero naming the first offending path otherwise.

# Why this is a file rather than three inline heredocs (#3272 review round 4)

The interleaving claim and its verdict fields were DELETED by owner ruling after four review
rounds in which each fix re-introduced the claim in new words. The property that must now
hold is a property of EVERY report this rig writes, so it is asserted from ONE place that
three suites call — a copy per call site is how the wording drifts and one copy stops
checking what the others do.

# Asked over KEYS, deliberately, not over the serialized text

The replacement prose legitimately contains the word "interleaving": the report says, in so
many words, that it makes NO interleaving claim. A substring scan over `json.dumps(...)`
would red on that disclaimer — which would push the disclaimer toward being vague, i.e. the
guard would degrade the very text it exists to protect. So the document is WALKED and each
KEY is compared exactly.

# The subject is stated affirmatively

`BANNED_KEYS` is not "some keys we remembered": it is the exact field set the deleted
`verify_interleaving`/`verify_round_major_timing`/`interleaving_lines` returned or printed,
listed beside the function it came from, so a reader can check the correspondence. A
document with no `recorded_round_metadata` at all is a FAILURE, not a pass — a report that
omitted the block entirely would otherwise satisfy a pure absence test (never derive a
positive verdict from the absence of a bad signal).
"""

from __future__ import annotations

import json
import pathlib
import sys

# Every field the DELETED code produced, by origin. Any of them in a report means the claim
# has come back.
BANNED_KEYS = {
    # from the deleted `verify_interleaving` return value
    "interleaving",
    "verified",
    "rotation_checked",
    "rotation_note",
    "positions_by_round",
    "arms_per_round",
    # from the deleted `verify_round_major_timing` return value
    "timing",
    "established",
    "round_major_verified",
    "within_round_span_ns",
    "max_within_round_span_ns",
    "rounds_compared",
    "not_established",
}

# Two of the names above are GENERIC enough to occur legitimately elsewhere in a report, so
# the exceptions are an EXPLICIT PATH ALLOWLIST rather than a narrower key set: dropping
# `verified` from `BANNED_KEYS` would stop this file noticing `interleaving.verified` coming
# back, which is the single field the whole deletion was about. An exemption is a path, so it
# cannot silently widen — a `verified` appearing anywhere else is still a finding.
#
# `$.pinning.verified` predates #3272's round 4 and describes the CPU-sibling verification
# (`thread_siblings_list`, fail-closed in scripts/perf/lib-cpu.sh), which is a real check that
# really runs. It is unrelated to interleaving.
ALLOWED_PATHS = frozenset({"$.pinning.verified"})

# The block that REPLACED them must be present, and must carry its no-claim marker. Absence
# of the banned keys is not sufficient evidence on its own: a report that wrote no round
# metadata at all would pass a pure absence test.
REQUIRED_PATH = "recorded_round_metadata"


def walk(node: object, path: str, offences: list[str]) -> None:
    if isinstance(node, dict):
        for key, value in node.items():
            child = f"{path}.{key}"
            if key in BANNED_KEYS and child not in ALLOWED_PATHS:
                offences.append(f"{child} is a DELETED interleaving verdict field")
            walk(value, child, offences)
    elif isinstance(node, list):
        for i, value in enumerate(node):
            walk(value, f"{path}[{i}]", offences)


def check(p: pathlib.Path) -> list[str]:
    doc = json.loads(p.read_text())
    offences: list[str] = []
    walk(doc, "$", offences)
    block = doc.get(REQUIRED_PATH)
    if not isinstance(block, dict) or not block:
        offences.append(
            f"$.{REQUIRED_PATH} is absent or empty — the replacement block must be"
            " PRESENT, else this check would pass on a report that recorded nothing"
        )
        return offences
    for temp, rec in block.items():
        if rec.get("claims_made") != "NONE":
            offences.append(
                f"$.{REQUIRED_PATH}.{temp}.claims_made is {rec.get('claims_made')!r},"
                " expected 'NONE'"
            )
        note = rec.get("claim_note", "")
        if "#3287/#3299" not in note:
            offences.append(
                f"$.{REQUIRED_PATH}.{temp}.claim_note does not point at #3287/#3299,"
                " where re-adding an OBSERVED control is tracked"
            )
        integrity = rec.get("integrity_checks", {})
        if not isinstance(integrity.get("round_pairs_compared"), int):
            offences.append(
                f"$.{REQUIRED_PATH}.{temp}.integrity_checks.round_pairs_compared must be"
                " an int COUNT (the honest scope), not absent"
            )
    return offences


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print(f"usage: {argv[0]} <results.json> [more.json …]", file=sys.stderr)
        return 2
    failed = False
    for arg in argv[1:]:
        p = pathlib.Path(arg)
        if not p.is_file():
            print(f"FAIL {p}: not a file (nothing was checked)", file=sys.stderr)
            failed = True
            continue
        offences = check(p)
        if offences:
            failed = True
            for line in offences:
                print(f"FAIL {p.name}: {line}", file=sys.stderr)
        else:
            print(f"ok {p.name}: no deleted verdict field; recorded block present")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
