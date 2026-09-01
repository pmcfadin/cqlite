# A/B construction assertion (issue #2824)

An A/B whose two arms differ in more than the property under test proves nothing while
looking exactly like proof. This file records that the two arms differ in **one** property,
asserted twice — once in the source, once at the syscall boundary.

## Both binaries come from ONE tree

Built by `docs/reports/issue-2824-artifacts/../..`-relative `/tmp/build-ab.sh` from a single
checkout at `46fe54cd3`: build patched -> revert **only** the `PrefetchMode::Auto` match arm
(`git diff --stat` = `1 file changed, 1 insertion(+), 1 deletion(-)`) -> build baseline ->
restore. Nothing else differs, including the toolchain, the dependency graph and the corpus.

An earlier pair was DISCARDED: the baseline had been built before an intervening rebase and the
patched after it, so the two came from different bases. The A/B would have run and produced a
plausible number.

| arm | sha256 |
|---|---|
| baseline (`Auto => None`) | `3542103a02b4fbf7ded59f75d0cff8660fa07d943177ccaafb744ffa61a79068` |
| patched (`Auto => Some(WillNeed)`) | `4488b5bc5973d15ae8dbc489eefd017f06baf1f5c6d45bf514eff34f2164eda7` |

## The arms differ by exactly one syscall

`strace -f -e trace=madvise` over `--setup-only` (reader open, which is where the advice is
issued) on the 2,774,760,422-byte `ws0/events` `Data.db`:

| advice | baseline | patched | what it is |
|---|---|---|---|
| `MADV_WILLNEED` | **0** | **1** | the change under test — the scan mapping |
| `MADV_RANDOM` | 1 | 1 | the #2210 dedicated point mapping, **untouched** |
| `MADV_SEQUENTIAL` | **0** | **0** | the #1143 prohibition, verified at RUNTIME |
| `MADV_DONTNEED` | 4 | 4 | **not ours** — see below |

Raw:

```
baseline: madvise(0x7368ee800000, 2774760422, MADV_RANDOM)   = 0

patched:  madvise(0x7f78b6800000, 2774760422, MADV_WILLNEED) = 0
          madvise(0x7f7811000000, 2774760422, MADV_RANDOM)   = 0
```

Three things this pins that no unit test does:

1. **Two distinct mappings exist and only the scan one is advised.** The addresses differ, and
   `MADV_RANDOM` lands on the other. This is the architecture the change depends on, observed
   rather than asserted from source.
2. **`MADV_SEQUENTIAL` is never issued**, by either arm, in a real run. The unit assert
   (`Auto != Sequential`) covers the policy function; this covers the whole open path.
3. **The four `MADV_DONTNEED` calls are not from this change.** They are 2,076,672-byte regions
   at thread-stack addresses — the allocator/runtime releasing thread stacks — and the count is
   **identical in both arms**, so they are neither a confound nor evidence that slice 1 shipped
   AC2. Slice 1 introduces no `MADV_DONTNEED`; that is asserted from the source diff, because
   a syscall-count grep could never distinguish ours from the runtime's.

## A scope fact this measurement makes concrete, and does not cover

`MADV_WILLNEED` is issued **over the entire 2.58 GiB file in a single call, at open**. For one
file on a 30 GiB box that is the intended behaviour and it fits. For a full-ring scan opening
many large SSTables concurrently it is a different proposition — each open queues whole-file
read-ahead, and file A's read-ahead can evict file B's hot pages, which is the warm-regression
direction AC1 forbids. **This single-file A/B cannot see that**, and no result in this directory
should be read as covering it. It is named as a residual for the i4i rig lane.
