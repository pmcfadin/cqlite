# Profiling-profile codegen fidelity — measured, and it falsified the first claim made about it

## Why this measurement exists

AC1 needs per-function attribution. `[profile.release]` sets `strip = true`, so the binary on the
measured path carries **no symbols** and cannot be attributed per-function. Any profiling profile
therefore differs from the binary whose throughput #3096 reported — and the size of that difference
decides whether AC1's numbers describe the same code as AC5's.

The first version of this work's method doc asserted the profiling profile "changes only debuginfo
emission and symbol retention" and was therefore codegen-identical. **That assertion was measured and
is false.** It is corrected here rather than removed, because the correction is the useful part.

## Measurement

`.text` section size, `size -A <binary>`, three profiles built from one unchanged tree:

| binary | `release` | `perfsym` (`debug=0, strip=none`) | `perfprof` (`debug=1, strip=none`) |
|---|--:|--:|--:|
| `cqlite-flight` | 8,304,695 | 8,306,231 | 8,317,687 |
| `flight-loadgen` | 8,536,439 | 8,536,119 | 8,550,071 |

Deltas against `release`:

| binary | `perfsym` | `perfprof` |
|---|--:|--:|
| `cqlite-flight` | **+1,536 B (+0.0185%)** | **+12,992 B (+0.156%)** |
| `flight-loadgen` | **−320 B (−0.0037%)** | **+13,632 B (+0.160%)** |

Symbol availability, `cqlite-flight`:

| profile | total symbols | Rust-v0 (`_RN…`) symbols |
|---|--:|--:|
| `release` | **0** | **0** |
| `perfsym` | 15,545 | 7,215 |
| `perfprof` | 15,548 | 7,218 |

## What this establishes

1. **`release` carries zero symbols — measured, not inferred.** Per-function attribution against it is
   impossible. `perf record` against such a binary exits 0 and `perf report` prints a well-formed table
   of raw addresses (`[.] 0x…`). This is a silent instrument failure of exactly the #3217 class, and it
   is a plausible partial explanation for why the encode region survived both #3217 and #3096
   unattributed: the default artifact on the measured path is not attributable, and nothing about
   running the profiler says so.

2. **Debuginfo is NOT codegen-neutral.** `debug = 1` moves `.text` by **+0.156%/+0.160%**, consistently
   upward on both binaries. So the two things a profiler wants have different costs, and only one of
   them is needed for AC1's headline figures:
   * the **symbol table** — what flat per-function attribution needs;
   * **debuginfo** — needed only for `perf annotate` source interleaving and `--call-graph dwarf`.

3. **Symbol retention alone is near-neutral, and its residual looks like layout noise rather than
   codegen divergence.** `perfsym` differs from `release` by **+0.0185%** on one binary and
   **−0.0037%** on the other — an order of magnitude smaller than `perfprof`, and **of opposite sign**.
   A systematic codegen change would not shrink one binary while growing the other; a linker/section
   layout difference would. This is offered as the *reading* of the residual, not as proof of it.

## Consequences adopted

* **AC1 headline per-function numbers come from `perfsym`** — the codegen-faithful, symbol-bearing
  vehicle.
* **`perfprof` (debuginfo) is used only for structural evidence** — call-graph shape, region membership,
  source-line annotation — and never as the source of a headline number, because it is the profile
  whose codegen is measurably furthest from `release`.
* **AC0 runs on plain `--release`**, as #3096 did. AC0 is a reproduction and must not silently change
  the binary.
* **`.text` parity is not sufficient evidence of throughput parity**, so the pre-registered
  codegen-equivalence control still runs: same-session interleaved throughput on the unprofiled arm,
  `release` vs `perfsym`. If they diverge beyond resolution, that is stated as a limitation on AC1
  rather than assumed away. `.text` size is a *proxy*; identical size would not prove identical code,
  and near-identical size does not prove identical speed.

## Note on what was NOT done

`-C force-frame-pointers=yes` was deliberately not set on either profile. It would give cheap, reliable
call-graph unwinding, but it **does** alter codegen — which is the property being protected here.
Call-graph runs use `--call-graph dwarf` on `perfprof` and are labelled structural-only.
