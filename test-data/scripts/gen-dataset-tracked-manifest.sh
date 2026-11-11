#!/usr/bin/env bash
# gen-dataset-tracked-manifest.sh <bundle.tar.gz> — write test-data/dataset-bundle-tracked.txt (#4508).
#
# The file lists the bundle members that are ALSO git-tracked in this checkout: the "tracked-class"
# files (jsonl oracles, logs, TOC) the bundle ships next to its untracked binaries. It is the
# bundle's own tracked set, pinned by sha256 (first line `# sha256=<pin>`), so it is TRUE BY
# CONSTRUCTION and independent of any cache or PR: a later PR that deletes or renames one of these
# files leaves the path in the manifest, which is exactly how fetch-datasets.sh recognises the
# extracted bundle copy as a stale twin (see lib/dataset-warm-restore.sh). Regenerate it in the PR
# that re-pins the dataset (bump-dataset-pin.sh), from the NEW bundle, on the checkout that PR
# produces; scripts/ci/check-dataset-pin.sh reds if the sha in the header is not the pin's.
set -euo pipefail
[ "$#" -eq 1 ] && [ -f "$1" ] || { echo "usage: $0 <bundle.tar.gz>" >&2; exit 2; }
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# shellcheck disable=SC1091
. "$ROOT/test-data/dataset-pin.env"
if command -v sha256sum >/dev/null 2>&1; then got="$(sha256sum "$1" | awk '{print $1}')"; else got="$(shasum -a 256 "$1" | awk '{print $1}')"; fi
[ "$got" = "$DATASET_SHA256" ] || { echo "ERROR: $1 is sha256 $got, not the pinned $DATASET_SHA256" >&2; exit 1; }
T="$(mktemp -d "${TMPDIR:-/tmp}/gen-tracked.XXXXXX")"; trap 'rm -rf "$T"' EXIT
tar -tzf "$1" | sed -n 's#^test-data/datasets/##p' | grep -v -e '/$' -e '^$' | LC_ALL=C sort -u > "$T/bundle"
git -C "$ROOT" ls-files -z -- test-data/datasets | tr '\0' '\n' | sed 's#^test-data/datasets/##' | LC_ALL=C sort -u > "$T/tracked"
LC_ALL=C comm -12 "$T/bundle" "$T/tracked" > "$T/both"
if grep -q -e '^/' -e '\.\.' -e $'\r' "$T/both"; then echo "ERROR: unsafe path in the bundle/tracked intersection" >&2; exit 1; fi
{ printf '# sha256=%s\n' "$DATASET_SHA256"; cat "$T/both"; } > "$ROOT/test-data/dataset-bundle-tracked.txt"
echo "wrote test-data/dataset-bundle-tracked.txt: $(wc -l < "$T/both" | tr -d ' ') tracked-class bundle member(s)"
