#!/usr/bin/env python3
"""The measured worker's dependency versions are PINNED, and the record agrees.

WHY THIS EXISTS. `scan-worker/` is its own workspace, so the repo's root
`Cargo.lock` does not pin its build, and its own lockfile is gitignored (roborev's
compiled-in `**/Cargo.lock` deny-list, #3278, makes a committed one unreviewable).
With caret ranges that combination left the binary that produced every published
number unrebuildable: a later `cargo build` resolves newer minors and measures
different codegen. The pin therefore lives in the manifest as `=`-exact
requirements, with the transitive closure recorded beside it — and THAT is a
claim, so it is checked rather than trusted.

Refusals (exit 3, `GUARD-FAIL <CODE>`), all fail-closed:

    PIN_MANIFEST_UNREADABLE  the manifest is missing or not TOML
    PIN_NOT_EXACT            a registry dependency is not `=x.y.z`
    PIN_LOCK_MISSING         the recorded closure is absent or not TOML
    PIN_LOCK_DISAGREES       the record names a version the manifest does not pin

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
        print("usage: check-exact-pins.py <Cargo.toml> [<recorded-lockfile>]", file=sys.stderr)
        return 2
    manifest = argv[1]
    lock = argv[2] if len(argv) == 3 else os.path.join(
        os.path.dirname(os.path.abspath(manifest)), "measured-build-lockfile.txt")

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
                 f"exact pin. This crate is outside the root workspace and its lockfile is "
                 f"gitignored, so a range here leaves the MEASURED binary unrebuildable.")
        pins[name] = req[1:]

    if not pins:
        fail("PIN_NOT_EXACT",
             f"{manifest} declares no registry dependencies to pin — this check would pass "
             f"vacuously, so it refuses instead.")

    recorded = load_toml(lock, "PIN_LOCK_MISSING", "the recorded dependency closure")
    have = {}
    for pkg in recorded.get("package") or []:
        have.setdefault(pkg.get("name"), set()).add(pkg.get("version"))
    for name, want in sorted(pins.items()):
        versions = have.get(name)
        if not versions:
            fail("PIN_LOCK_DISAGREES",
                 f"the manifest pins {name} ={want} but the recorded closure {lock} does not "
                 f"contain {name} at all — the record is not of this build.")
        if want not in versions:
            fail("PIN_LOCK_DISAGREES",
                 f"the manifest pins {name} ={want}; the recorded closure {lock} holds "
                 f"{name} {sorted(versions)}. One of the two is stale.")
    print(f"exact pins OK: {len(pins)} registry deps "
          f"({', '.join(f'{k}={v}' for k, v in sorted(pins.items()))}); "
          f"recorded closure {os.path.basename(lock)} agrees ({len(have)} packages)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
