# Design — readat-positional

## Context

Source of truth: `docs/reports/read-path-performance-audit-2026-07-01.md` §Epic C; issue #1573
(child C2 of Epic C #1515). Anchors as of `main` @ `5c080d2a`.

**Owner Decision #3 (2026-07-01):** the `ReadAt` sync-core refactor is endorsed as the target
read architecture. This design records that endorsed target; it does not re-litigate the call
or weigh alternatives — the architecture is locked.

Guardrails carried from the issue: do NOT rewrite the windowed streaming scan; CRC-then-decompress
ordering and error classification unchanged; do NOT flip `use_mmap` here.

## Decision 1 — the `ReadAt` trait (endorsed target; replaces the cursor-mutex)

Introduce a positional-read trait in the reader I/O layer:

```
trait ReadAt {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<usize>;
    fn read_exact_at(&self, offset: u64, buf: &mut [u8]) -> Result<()>;
}
```

Invariants (the whole point of the refactor):
- **No mutable position** — the trait carries no seek cursor.
- **No `&mut self`** — every method takes `&self`, so concurrent callers share one value with
  no exclusive borrow and no mutex.
- **No seek** — the offset is a parameter of the read, not shared file state.

This **replaces** the `Arc<Mutex<BlockSource>>` cursor on the point-read path. Today the mutex
exists solely to serialize access to a mutable seek position and is held **across disk I/O**
(`types.rs:217`, `data_access/mod.rs:428-445`), which is exactly the convoy. A positioned read
on a shared fd is independent of every other positioned read, so the lock is not merely moved —
it is removed from this path entirely.

## Decision 2 — per-backend implementations

| Backend | `read_at` implementation | Notes |
|---------|--------------------------|-------|
| buffered / plain | `std::os::unix::fs::FileExt::read_at` on ONE shared `File` | A shared fd's positioned reads are independent — no lock needed. Replaces per-op `File::open`. |
| mmap | slice indexing into the resident map | Bounds-checked slice read; zero syscalls per read. |
| O_DIRECT | positioned read honoring the existing 4K alignment logic | Reuse the alignment/bounce-buffer math already present in the direct backend; only the seek+read becomes a positioned read. |
| Windows | `std::os::windows::fs::FileExt::seek_read`, cfg-gated | Matches the platform cfg-gating pattern already used by the existing backends. |

## Decision 3 — migrate the point-read path onto `ReadAt`

- **BIG chunk fetch:** resolve the query key's partition offset → the chunk via
  `CompressionInfo::chunk_for_offset` → `read_at` that chunk's byte range → CRC check →
  decompress. Remove the `Mutex<BlockSource>` from this path. CRC-then-decompress ordering is
  preserved verbatim.
- **BTI lookups:** open the fd **once at reader open** and store the `ReadAt` backend on the
  reader; every subsequent lookup issues positioned reads instead of `File::open`
  (`source.rs:108-130`). This eliminates the per-lookup open/close and the fd-exhaustion class.
- Offset resolution logic (bloom/trie prune, `chunk_for_offset` at `bti.rs:600-640`) is
  unchanged — only the byte-fetch mechanism moves from cursor+seek / per-op-open to `read_at`.

## Decision 4 — scans left on the existing pipeline (scoped, per guardrail)

The windowed streaming scan is not migrated. The trait is intentionally shaped so a scan can
later hold a `ReadAt` and issue positioned window reads (F3), but that migration is out of scope
here. Scans remain functionally unchanged; their parity/behavior is asserted, not modified.

## Risks

- **fd lifetime:** the once-opened BTI fd must live for the reader's lifetime; store it on the
  reader (owned `File` behind the `ReadAt` impl) so it closes on reader drop — no leak, no churn.
- **O_DIRECT alignment:** the positioned-read path must keep the existing 4K alignment handling;
  covered by reusing the current alignment logic and by the positional-read correctness scenario.
- **Windows drift:** cfg-gated `seek_read` is compiled but exercised only where the workspace
  builds on Windows; the correctness scenarios run on the Unix backends in CI.
