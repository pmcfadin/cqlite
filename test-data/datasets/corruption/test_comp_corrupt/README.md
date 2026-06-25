# test_comp_corrupt — Corrupted-component fixture corpus (epic #970, issue #999)

Deterministic single-component corruptions of the clean `test_comp` (nb/BIG)
and `test_da` (BTI/`da`) fixtures. Each subdirectory is a COMPLETE SSTable
component directory copied from a clean source with EXACTLY ONE intentional
mutation applied to ONE component.

- Generator: `test-data/scripts/generate-corruption-corpus.sh` (no Docker; pure offline transform)
- Manifest : `corruption-manifest.yml` (machine-readable; committed)
- Report   : `verification-report.txt` (single-mutation proof + SHA256 before/after; committed)

## Regenerate (byte-identical)

```bash
bash test-data/scripts/generate-corruption-corpus.sh
# verify only:
bash test-data/scripts/generate-corruption-corpus.sh --verify-only
```

The corrupted `*.db` binaries are **gitignored** (like all clean `*.db`). They
are regeneratable byte-for-byte from the committed manifest + clean sources, so
CI consumes the DESCRIBED corruptions and never mutates bytes at test time.
