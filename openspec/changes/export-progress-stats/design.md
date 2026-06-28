# Design — Export progress reporting & final statistics (#284)

## Context

`export_data()` in `cqlite-cli/src/commands/export.rs` already:
- computes `show_progress = !quiet && stdout().is_terminal()`,
- builds an **indeterminate** `ProgressBar::new_spinner()` (template
  `{spinner:.green} {msg} ({pos} rows)`) or `ProgressBar::hidden()`,
- streams rows in chunks, calling `pb.set_position(rows_exported)` per chunk,
- prints a final summary (`rows`, `format_bytes(file_size)`, `format_export_duration`,
  `rate`) only when `!quiet`.

`indicatif` is already a dependency. So the work is **narrow**: choose a determinate bar
when a total is known, render ETA, and pin the contract with tests.

## The one real decision: what is the "total"?

A determinate bar + ETA requires a denominator. The streaming engine does **not** know the
result-set size up front, so we must pick a source for the total. Options considered:

| Option | Total source | Pros | Cons |
|---|---|---|---|
| **A — `--limit` only (chosen)** | `limit` when the user passes `--limit N` | Authoritative (user-stated ceiling); zero extra I/O; no heuristic; trivially correct percent + ETA | No determinate bar for un-limited full-table exports (they keep the spinner) |
| B — Statistics.db row estimate | SSTable `Statistics.db` partition/row estimate | Determinate bar for full-table exports | It is an **estimate** (bar can exceed 100% or stall near the end); wrong for `WHERE`/`SELECT`; needs plumbing the estimate through the query engine; smells of heuristic UX |
| C — Pre-count pass | A first scan that counts rows | Always exact | Doubles I/O/time for a cosmetic; unacceptable for large exports (the exact case #284 cares about) |

**Chosen: Option A.** It is the only source that is authoritative and free, and it aligns
with the no-heuristics mandate — we never *infer* a total from data shape. When `--limit N`
is supplied, the export will write at most `N` rows (the loop already enforces this), so `N`
is an exact, honest denominator. Everything else falls back to the existing spinner.

> Note: the export builds the query by appending `LIMIT n` (table source) or respecting an
> existing/added `LIMIT` (SELECT source). For the determinate bar we use the **CLI `--limit`
> value** as the denominator, not by re-parsing SQL — that keeps the total decision in one
> place. If the source is a raw `SELECT ... LIMIT k` with no CLI `--limit`, the total stays
> unknown (spinner) — we do not parse user SQL to extract a limit in this change.

Option B is explicitly left as a possible follow-up (a clearly-labeled "~est." bar); it is
out of scope here so we don't ship estimate-as-fact progress.

## Behavior contract (rendered as scenarios in the spec)

Decision table for a single export invocation:

| `--quiet` | stdout TTY? | `--limit` set? | Progress shown | Final summary |
|---|---|---|---|---|
| no | yes | yes | **Determinate bar** (percent, pos/len, ETA) | yes |
| no | yes | no | Spinner (pos rows, no ETA) | yes |
| no | no (piped) | any | none | none |
| yes | any | any | none | none |

- **ETA** is rendered only by the determinate bar (indicatif's `{eta}` token), i.e. only when
  `--limit` gives a total. We never print an ETA for the spinner (no honest basis for it).
- **Final summary** is governed by the existing `!quiet` print, but to honor the "no output
  when piped" rule it MUST also be gated on the same TTY check as progress (today the summary
  prints whenever `!quiet`, even when piped — this change tightens it to `show_progress`/TTY).

## Implementation sketch (for flow-implement, not built here)

In `export_data()`:
1. Replace the always-spinner block with a small helper:
   ```rust
   fn make_progress(show: bool, total: Option<u64>) -> ProgressBar {
       if !show { return ProgressBar::hidden(); }
       match total {
           Some(n) => {
               let pb = ProgressBar::new(n);
               pb.set_style(ProgressStyle::default_bar()
                   .template("[{bar:40}] {percent}% ({pos}/{len}) ETA: {eta}")?  // handled w/o unwrap in lib-safe way
                   .progress_chars("##-"));
               pb
           }
           None => { /* existing spinner */ }
       }
   }
   ```
   `total = limit.map(|n| n as u64)`.
2. The per-chunk `pb.set_position(rows_exported)` call already drives both bar and spinner —
   no loop change needed.
3. `pb.finish_and_clear()` on completion (already present in branches).
4. Gate the final summary on the TTY/`show_progress` check (tighten from `!quiet`).
5. Keep `format_bytes` / `format_export_duration` for the summary.

This touches only `cqlite-cli/src/commands/export.rs` (and its test file). `export.rs` is
within the file-size budget; no split needed. No `unwrap()`/`expect()` in non-test code
(template-build errors handled, not unwrapped).

## Testing strategy

- **Wiring-evidence (CLI, public surface):** an `assert_cmd` test in
  `cqlite-cli/tests/export_integration_tests.rs` that runs the real `export` subcommand and
  asserts: (a) with `--limit` on a TTY-like run the summary block (`Rows:`/`Size:`/`Time:`/
  `Rate:`) is present on stdout; (b) piped / `--quiet` runs emit an empty stdout while still
  producing the output file. (assert_cmd's captured stdout is non-TTY, which directly
  exercises the suppression path; a forced-render path is used to assert the bar contents —
  see below.)
- **Unit test (decision logic):** a pure test for the total-resolution + ETA-eligibility
  function — `--limit Some(n)` → determinate (total `Some(n)`, ETA on); `None` → spinner
  (total `None`, ETA off) — so the contract is verified without a TTY. This isolates the one
  real decision from terminal detection.
- The gate runs the cli test component; integration tests need `CQLITE_DATASETS_ROOT`.

## Risks

- `assert_cmd` always captures a non-TTY stdout, so the determinate-bar *rendering* can't be
  observed through it directly — that's why the bar/ETA decision is factored into a unit-
  testable function (the suppression behavior is still verified end-to-end via the CLI test).
