# VInt width distribution, MEASURED (issue #3445)

`codegen-fingerprint.md` recorded that the multi-byte path issues a dynamic-length
`call memcpy@GLIBC`, and stated that whether that path is ever reached in a measured scan is
an EMPIRICAL question rather than something to assume in either direction. This file
answers it, so that commitment is discharged by measurement.

## How

A THROWAWAY probe (`vint-width-probe.patch`, applied only in a detached `git worktree`,
never on this branch) adds a relaxed-atomic counter per decoded width inside
`decode_unsigned` and dumps the histogram to stderr. The probe PERTURBS the hot loop, so
**its timing is discarded and only its distribution is used** — no number in this report's
cycle accounting comes from this build.

## Result

Snapshot taken at the dump instant (counts are point-in-time; the RATIO is the result):

| VInt width | decodes | share | path taken |
|---|--:|--:|---|
| 1 byte  | 100,000,000 | 55.62% | single-byte fast return |
| 2 bytes | 59,835,647 | 33.28% | memcpy + bswap + mask/shift |
| 3 bytes | 10,429,487 | 5.80% | memcpy + bswap + mask/shift |
| 4 bytes | 9,534,252 | 5.30% | memcpy + bswap + mask/shift |
| 5 bytes | 0 | 0.00% | memcpy + bswap + mask/shift |
| 6 bytes | 0 | 0.00% | memcpy + bswap + mask/shift |
| 7 bytes | 1 | 0.00% | memcpy + bswap + mask/shift |
| 8 bytes | 0 | 0.00% | memcpy + bswap + mask/shift |
| 9 bytes | 1 | 0.00% | memcpy + bswap + mask/shift |
| **total** | **179,799,388** | 100% | |

- **single-byte (fast return, no `bswap`): 55.6%**
- **multi-byte (memcpy + `bswap` path): 44.4%**
- 5 bytes and wider: 0.0000% — effectively absent

## Why this matters to the numbers above it

It explains the single most striking figure in the cycle attribution: **one `bswap`
instruction carries 72.7% of all VInt decode cycles**. That instruction exists only on the
multi-byte path, and the multi-byte path is taken by 44% of decodes — so the
concentration is not an artifact of sample skid landing on a convenient instruction, it is
where the work actually is. The `bswap` is also the consumer of a store-to-load forward from
the `memcpy` staging buffer, which is the most plausible reason cycles pile there rather than
on the arithmetic around it.

It also bounds the ceiling of any future VInt lever, which is the only reason to record it in
a KILL report: a change that made multi-byte decoding free could not remove more than the
VInt share itself, and 56% of decodes never touch that path at all.

## Rate, stated as a bound rather than a point value

The dump fires when the 1-byte bucket crosses a multiple of 25M, at an instant that cannot be
aligned to a row count, so decodes-per-row is reported as a LOWER BOUND rather than a figure
that looks more precise than it is: 179,799,388 decodes against the run's final
15,320,832 rows (11,320,832 measured + 4,000,000 prewarm) is **>= 11.7 VInt
decodes per row**.

