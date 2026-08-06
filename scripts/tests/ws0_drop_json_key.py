#!/usr/bin/env python3
"""`ws0_drop_json_key.py <json> <key>` — the same object with one key REMOVED.

A test helper, extracted to a file rather than written inline (#3272 review round 11, F3). The
inline form was a `python3 -c '…'` inside a shell loop inside a `<<'PY'` heredoc, which is three
nested quoting contexts, and the failure mode of getting that wrong is not a syntax error — it is a
SILENTLY TRUNCATED test file, which is exactly what `lib-ws0-fixtures.sh` records having happened
once already (an apostrophe inside a single-quoted `python3 -c` body truncated the whole library and
presented as every other case failing on an unrelated missing artifact).

Absence is the point: several #3272 refusals are about a field that was NOT OBSERVED, and a fixture
that has to construct "the healthy record minus exactly one key" needs to do it from the healthy
record rather than by maintaining a second literal per case — a second literal drifts, and the case
then tests a record nobody uses.

Exits non-zero if the key is not there: silently returning the object unchanged would make the
case's subject (the field's ABSENCE) unreachable while the case still passed for the wrong reason.
"""

from __future__ import annotations

import json
import sys


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        print("usage: ws0_drop_json_key.py <json-object> <key-to-drop>", file=sys.stderr)
        return 2
    try:
        rec = json.loads(argv[1])
    except ValueError as exc:
        print(f"not valid JSON: {exc}", file=sys.stderr)
        return 2
    if not isinstance(rec, dict):
        print(f"expected a JSON object, got {type(rec).__name__}", file=sys.stderr)
        return 2
    key = argv[2]
    if key not in rec:
        # A fixture asking to remove a key that is already absent is asking for a record it thinks
        # differs from the healthy one and does not. Refused, rather than returning the input: the
        # case would otherwise pass while testing nothing.
        print(f"{key!r} is not in the record, so removing it would produce the SAME object and the"
              " case's subject (its absence) would be untested", file=sys.stderr)
        return 2
    del rec[key]
    print(json.dumps(rec))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
