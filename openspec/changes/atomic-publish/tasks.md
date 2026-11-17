# Tasks — atomic-publish (#4529)

- [x] `write_engine::rebuild::staged_publish` (stage, fsync, rename with Data.db last, dir fsync, leftover refusal)
- [x] `rebuild_components` and `salvage_sstable` write into the stage and publish
- [x] CLI refusal names the leftover staging directory
- [x] `cqlite-cli/tests/issue_4529_atomic_publish.rs` (BIG + BTI, both verbs, every rename boundary)
- [x] Owner ruling 2026-11-16 recorded (AC1 reworded, scenarios amended; test asserts it unchanged)
- [x] After #4498 merges: un-ignore its `issue_4529_*` harness tests and drop their known-defect exclusion (commits 10431120d, 52d4e800e)
- [x] Durability barrier: fsync destination directories after every non-Data.db rename, before the Data.db rename, and again after; fsync created intermediate directories and the parent of a created `--out`
- [x] Pre-flight collision check over every destination before the first rename
- [x] Unit tests pin the operation order, the pre-flight refusal, and that `Drop` is inert after a completed publish
