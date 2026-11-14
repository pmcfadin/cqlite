# Tasks — atomic-publish (#4529)

- [x] `write_engine::staged_publish` (stage, fsync, rename with Data.db last, dir fsync, leftover refusal)
- [x] `rebuild_components` and `salvage_sstable` write into the stage and publish
- [x] CLI refusal names the leftover staging directory
- [x] `cqlite-cli/tests/issue_4529_atomic_publish.rs` (BIG + BTI, both verbs, every rename boundary)
- [ ] After #4498 merges: un-ignore its `issue_4529_*` harness tests and drop their known-defect exclusion
