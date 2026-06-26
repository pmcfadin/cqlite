# Runbook — #1099 Path A: republish `cassandra5-small-full` with Epic #970 fixtures

**Goal:** the pinned CI dataset asset (`cassandra5-small-full-v3.1.tar.gz`, tag
`datasets-v3`) predates Epic #970 (PR #1091), so it lacks the `test_comp` and
`corruption/test_comp_corrupt` binaries that `issue_1000_verifier.rs` requires.
Strict/nightly lanes (and any future lane that sets `CQLITE_REQUIRE_FIXTURES=1`)
therefore can't exercise compression/corruption parity. This runbook regenerates
the corpus **with** those fixtures, publishes a `v3.2` asset, and repoints CI.

> The PR-lane `test` failure was already mitigated separately (#1094 skip-on-
> presence). This is the durable fix so the fixtures are actually present.

**Prerequisites:** Docker (or Podman) with `cassandra:5.0.2` pullable, ~10 GB
free disk, ~4 GB RAM for the container, and `gh` authenticated as a user with
release-upload rights to `pmcfadin/cqlite`. Run from a clean checkout of `main`.

---

## 1. Generate the Epic #970 fixtures (Docker / Cassandra 5.0.2)

These are the same steps the strict nightly workflow
(`.github/workflows/compression-corruption-parity.yml`) runs. They write into
`test-data/datasets`. Run from the repo root:

```bash
export CQLITE_DATASETS_ROOT="$PWD/test-data/datasets"

# Start from the current canonical corpus so nb/oa/da keyspaces are present.
# (The Epic #970 fixtures are added alongside them in disjoint keyspaces.)
bash test-data/scripts/fetch-datasets.sh        # restores v3.1 nb/oa/da binaries

# test_comp keyspace (compression scenarios) — deterministic, re-runnable.
bash test-data/scripts/generate-compression-parity.sh

# BTI source (test_da/wide_table) needed by the BTI corruption cases.
OUT="$CQLITE_DATASETS_ROOT" bash test-data/scripts/gen-wide-bti.sh

# Corruption corpus (corruption/test_comp_corrupt) + regenerated manifest.
bash test-data/scripts/generate-corruption-corpus.sh
```

Verify the fixtures materialized (mirror of the workflow's strict assert):

```bash
find "$CQLITE_DATASETS_ROOT/sstables/test_comp" -name '*-Data.db' | wc -l        # expect >= 7
find "$CQLITE_DATASETS_ROOT/corruption/test_comp_corrupt" -mindepth 1 -maxdepth 1 -type d | sort
```

Confirm they pass verification locally (strict mode — fail instead of skip):

```bash
env CQLITE_REQUIRE_FIXTURES=1 CQLITE_DATASETS_ROOT="$CQLITE_DATASETS_ROOT" \
  cargo test -p cqlite-core --features write-support,cli-helpers \
  --test issue_1000_verifier
```

## 2. Package the full corpus as the v3.2 asset

`package_datasets.sh` tars `test-data/datasets`, validates it contains `.db`
binaries, and writes `<archive>.sha256`:

```bash
ASSET=cassandra5-small-full-v3.2.tar.gz
ASSET_NAME="$ASSET" test-data/scripts/package_datasets.sh \
  --type full --asset-name "$ASSET" --tag datasets-v3

# Archive + checksum land at the repo parent dir:
cat "../$ASSET.sha256"          # <-- copy this 64-char SHA256 for step 4
```

## 3. Publish to the GitHub release

Upload the new versioned asset to the **existing** `datasets-v3` release (it can
hold multiple assets; keeping the tag avoids cache-key churn). `--clobber`
replaces an asset of the same name if you re-run:

```bash
gh release upload datasets-v3 "../$ASSET" "../$ASSET.sha256" --clobber
```

> If you prefer a brand-new release tag instead, create it and pass `--new-tag`
> in step 4.

## 4. Repoint CI at the new asset (10 workflows)

Use the finalizer — it replaces the asset filename + SHA256 across every
pinned workflow **and** `test-data/scripts/fetch-datasets.sh` (which carries its
own `DATASET_ASSET`/`DATASET_SHA256`/`DATASET_TAG` defaults), then verifies no
stale reference remains. With `--new-tag` it also rewrites the inline
`gh release download <tag>` / `releases/download/<tag>/` literals (coverage.yml,
m1-ci.yml). It never edits its own `OLD_*` defaults or this runbook, and prints
any doc-only references (website docs) for you to update by hand:

```bash
git switch -c chore/1099-bump-dataset-pin-v3.2
test-data/scripts/bump-dataset-pin.sh --new-sha <sha256-from-step-2>
# (add --new-tag <tag> if you cut a new release tag)

# Stage everything the finalizer touched (workflows AND the fetch helper):
git add .github/workflows test-data/scripts/fetch-datasets.sh
git commit -m "ci: pin dataset asset to cassandra5-small-full-v3.2 (#1099)"
git push -u origin chore/1099-bump-dataset-pin-v3.2
gh pr create --base main --fill
```

The 10 pinned workflows: `ci.yml`, `coverage.yml`, `docs-site.yml`,
`m1-ci.yml`, `node-ci.yml`, `observability-gate.yml`, `perf-regression.yml`,
`python-ci.yml`, `smoke-tests.yml`, `sstabledump-parity-gate.yml`.

> **Cache note:** `coverage.yml`/`m1-ci.yml` restore the dataset cache and skip
> the download when `test_basic/simple_table-*-Data.db` is already present.
> Their dataset cache `restore-keys` are **SHA-scoped** (`datasets-v3-<sha>-`),
> so a SHA bump can never warm-restore an older dataset version and silently
> skip the re-download — the finalizer rewrites that SHA along with the rest.

## 5. Verify

Once the pin-bump PR's CI is green (the cache busts automatically because the
key includes the SHA), the `test` lane runs the verifier against the real
`test_comp`/corruption binaries instead of skipping. Optionally trigger the
strict nightly manually (`compression-corruption-parity.yml` → workflow_dispatch)
to confirm `CQLITE_REQUIRE_FIXTURES=1` passes end to end. Then close #1099.
