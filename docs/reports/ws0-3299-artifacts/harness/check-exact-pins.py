#!/usr/bin/env python3
"""The measured worker's dependency versions are PINNED, and the lockfile agrees.

WHY THIS EXISTS. `scan-worker/` is its own workspace, so the repo's root
`Cargo.lock` does not pin its build. The TRANSITIVE closure is pinned by the
crate's OWN `Cargo.lock`, which is committed beside the manifest and which
`sweep.sh` builds against with `--locked`, so a drifted lockfile FAILS the build
rather than being silently re-resolved.

This check covers the other half, which `--locked` does not: `--locked` proves
the build matched SOME committed lockfile, not that the manifest states an exact
requirement. `=`-exact DIRECT pins are belt and braces — they make the intended
version legible in the manifest a reviewer reads (roborev's compiled-in
`**/Cargo.lock` deny-list, #3278, means the lockfile itself never reaches a
review diff) — and the manifest and the lockfile are asserted to AGREE, so
neither can drift alone.

Refusals (exit 3, `GUARD-FAIL <CODE>`), all fail-closed:

    PIN_MANIFEST_UNREADABLE  the manifest is missing or not TOML
    PIN_NOT_EXACT            a registry dependency is not `=x.y.z`
    PIN_LOCK_MISSING         the lockfile is absent or not TOML
    PIN_LOCK_DISAGREES       the lockfile names a version the manifest does not pin

Path dependencies are EXEMPT and must be: they are pinned by the repo commit, and
a version requirement on them would be a second, driftable pin.
"""

import os
import re
import sys
import tomllib

EXACT = re.compile(r"^=\d+\.\d+\.\d+$")


def fail(code, msg):
    print(f"GUARD-FAIL {code}: {msg}", file=sys.stderr)
    sys.exit(3)


def load_toml(path, code, what):
    try:
        with open(path, "rb") as fh:
            return tomllib.load(fh)
    except (OSError, tomllib.TOMLDecodeError) as exc:
        fail(code, f"cannot read {what} at {path}: {exc}")


def main(argv):
    if len(argv) not in (2, 3):
        print(__doc__, file=sys.stderr)
        print("usage: check-exact-pins.py <Cargo.toml> [<lockfile>]", file=sys.stderr)
        return 2
    manifest = argv[1]
    lock = argv[2] if len(argv) == 3 else os.path.join(
        os.path.dirname(os.path.abspath(manifest)), "Cargo.lock")

    doc = load_toml(manifest, "PIN_MANIFEST_UNREADABLE", "the worker manifest")
    pins = {}
    for name, spec in (doc.get("dependencies") or {}).items():
        if isinstance(spec, dict):
            if "path" in spec:
                if "version" in spec:
                    fail("PIN_NOT_EXACT",
                         f"{name} is a PATH dependency carrying a version requirement "
                         f"({spec['version']!r}). It is pinned by the repo commit; a second "
                         f"pin can only drift from the first.")
                continue
            req = spec.get("version")
        else:
            req = spec
        if not isinstance(req, str) or not EXACT.match(req):
            fail("PIN_NOT_EXACT",
                 f"registry dependency {name} requires {req!r}, which is not an `=x.y.z` "
                 f"exact pin. This crate is outside the root workspace, so the root lockfile "
                 f"does not reach it and a range here leaves the intended version unstated in "
                 f"the one file a reviewer receives.")
        pins[name] = req[1:]

    if not pins:
        fail("PIN_NOT_EXACT",
             f"{manifest} declares no registry dependencies to pin — this check would pass "
             f"vacuously, so it refuses instead.")

    recorded = load_toml(lock, "PIN_LOCK_MISSING", "the committed lockfile")
    have = {}
    for pkg in recorded.get("package") or []:
        have.setdefault(pkg.get("name"), set()).add(pkg.get("version"))
    for name, want in sorted(pins.items()):
        versions = have.get(name)
        if not versions:
            fail("PIN_LOCK_DISAGREES",
                 f"the manifest pins {name} ={want} but the lockfile {lock} does not contain "
                 f"{name} at all — the lockfile is not of this manifest.")
        if want not in versions:
            fail("PIN_LOCK_DISAGREES",
                 f"the manifest pins {name} ={want}; the lockfile {lock} holds "
                 f"{name} {sorted(versions)}. One of the two is stale.")
    print(f"exact pins OK: {len(pins)} registry deps "
          f"({', '.join(f'{k}={v}' for k, v in sorted(pins.items()))}); "
          f"lockfile {os.path.basename(lock)} agrees ({len(have)} packages)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
