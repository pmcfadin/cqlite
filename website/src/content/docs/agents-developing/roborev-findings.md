---
title: Common roborev findings and how to pre-empt them
description: The recurring roborev finding classes — and the one-line fix pattern for each — so implementations land clean and reviews converge in fewer rounds. (Issue #1245)
sidebar:
  label: Pre-roborev self-check
  order: 9
---

`roborev_findings` is the #1 recurring delivery cost in the pipeline telemetry retro
(`docs/reports/delivery-telemetry.jsonl`). Most rounds are spent re-litigating the same
handful of finding classes. Scan your diff against this checklist **before** reporting an
implementation done — every one pre-empted is a review round saved.

This mirrors the **Pre-roborev self-check** section in `CLAUDE.md`. Keep both in sync.

## The recurring finding classes

### GitHub Actions command injection
User- or dispatch-controlled input (`${{ inputs.* }}`, `${{ steps.*.outputs.* }}`)
interpolated directly into a `run:` shell — worst in a step that holds secrets in `env`.

**Fix:** allowlist-validate the value fail-closed *before* any secret step, then pass it
through a quoted env var; never inline `${{ }}` in `run:`.

```yaml
# Not allowed — injection sink
- run: ./gradlew publish -Pversion=${{ inputs.version }}

# Allowed — validate fail-closed, then quoted env var
- env:
    VERSION: ${{ inputs.version }}
  run: |
    [[ "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || { echo "bad version"; exit 1; }
    ./gradlew publish -Pversion="$VERSION"
```

### clippy `manual_range_contains`
`x >= a && x <= b` fails under `RUSTFLAGS="-D warnings"`.

**Fix:** `(a..=b).contains(&x)`.

### Integer overflow / saturation
Decoding into `i128` or a fixed width and saturating (decimal unscaled values, scale math)
silently loses data; materializing `10^scale` with an unbounded exponent is a DoS/OOM risk.

**Fix:** use `num_bigint::BigInt` (already a dependency) and bound the computation —
compare signs and adjusted exponents *before* computing any large power of ten.

### Float ordering vs Java
Rust `total_cmp` does not match Java `Float.compare` / `Double.compare`: Rust orders
negative NaN first, Java sorts NaN last; signed-zero handling also differs.

**Fix:** when matching Cassandra ordering, use an explicit comparator — NaN last,
`-0.0 < +0.0`.

### Wall-clock races in tests
Asserting a value sampled at one instant against a window captured at a different instant
flakes on one-second boundaries.

**Fix:** capture the time window so it covers *all* sampled operations (sample the bounds
around the whole block, not per-call). If the assertion is really a *perf* signal, convert it to a
recorded metric (`eprintln!`) that belongs to the benchmark lane rather than the correctness gate —
that is how #2369's `collection_benchmarks` wall-clock bounds were retired.

### Process-global work counters under thread-parallel tests
A test that asserts a **delta** on a process-global counter (an `AtomicU64` incremented deep in the
read/scan path) flakes under CI's thread-parallel `cargo test` — unrelated concurrent tests bump the
same counter between the before/after reads. `#[serial(tag)]` only serializes same-tag tests, so an
untagged sibling still contaminates the delta. Local per-process runners (nextest) never reproduce it.

**Fix (structural):** scope the measurement to the current thread — a `#[cfg(test)]` thread-local
scope guard (the `StreamWalkScope` pattern, #2428; `index_probes` follow-up #2451) that reads only
its own thread's increments, contamination-proof by construction. Production builds keep the plain
atomic. Serial tags on the counter then become redundant.

### No-heuristics violations
Inferring a type or behaviour from byte patterns instead of authoritative metadata.

**Fix:** decode from schema or `Statistics.db` metadata only. See the
[no-heuristics mandate](/cqlite/agents-developing/no-heuristics/).

### Gitignored reference binaries / dirty-tree gate
Byte-parity tests silently **SKIP** in a clean checkout because their `.db` references are
gitignored — so a gate that "passed" against your dirty working tree proves nothing.

**Fix:** force-add the tiny reference binaries (`git add -f`) and verify the test against a
fresh `git worktree add --detach HEAD`, never the dirty tree.

## How to use this

1. Before handing an issue off, diff your branch against `origin/main` and walk this list.
2. Fix matches up front rather than waiting for roborev to flag them.
3. Then run `scripts/agent-gate.sh` and request review as usual — see the
   [gate contract](/cqlite/agents-developing/gate-contract/).

## Running the codex reviewer — override BOTH agent and model

`.roborev.toml` on `main` pins `agent = 'claude-code'` and `review_model = 'opus'`. To run the codex
backend you must override **both** on the command line:

```bash
roborev review --branch --base origin/main --agent codex --model gpt-5.6-sol --wait
```

`--agent codex` **alone** still inherits `review_model = 'opus'` from config, and codex on a ChatGPT
account rejects `opus` with a hard `400 'opus' model is not supported` — a silent review failure that
looks like a backend outage rather than a config mismatch. Worktrees inherit `main`'s pinned config, so
`--model gpt-5.6-sol` (codex's own configured model, from `~/.codex/config.toml`) is the reliable
override on every checkout.
