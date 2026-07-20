use cqlite_core::storage::commitlog::{CommitLogSchema, SchemaSet};
// re-derive a quick LCG so we don't add a rand dep
fn lcg(s: &mut u64) -> u64 { *s = s.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407); *s }
#[test]
fn never_panics_on_random_and_mutated_real_bytes() {
    use cqlite_core::storage::commitlog::CommitLogReader;
    // Seed corpus: the real clean fixture bytes.
    let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../test-data/datasets/commitlog");
    let mut seed_bytes = Vec::new();
    if let Ok(rd) = std::fs::read_dir(&dir) {
        for e in rd.flatten() {
            let n = e.file_name().to_string_lossy().into_owned();
            if n.starts_with("clean-") && n.ends_with(".log") {
                seed_bytes = std::fs::read(e.path()).unwrap();
            }
        }
    }
    let _ = CommitLogSchema { keyspace:"k".into(), table:"t".into(), partition_key:vec![], clustering:vec![], columns:vec![] };
    let _schemas = SchemaSet::new();
    let mut st = 0x1234_5678_9abc_def0u64;
    for iter in 0..20000u64 {
        let mut data: Vec<u8> = if iter % 3 == 0 && !seed_bytes.is_empty() {
            let mut d = seed_bytes.clone();
            // flip a handful of random bytes
            for _ in 0..8 { let i = (lcg(&mut st) as usize) % d.len(); d[i] = lcg(&mut st) as u8; }
            // random truncation
            let cut = (lcg(&mut st) as usize) % d.len(); d.truncate(cut.max(1)); d
        } else {
            let len = (lcg(&mut st) % 256) as usize;
            (0..len).map(|_| lcg(&mut st) as u8).collect()
        };
        // write to a temp file and open (exercises the public reader path too)
        let p = std::env::temp_dir().join(format!("cl-fuzz-{iter}.log"));
        std::fs::write(&p, &data).unwrap();
        if let Ok(r) = CommitLogReader::open(&p) {
            let mut it = r.mutations();
            let mut g = 0; while let Some(_res) = it.next() { g+=1; if g>100000 {break;} }
        }
        let _ = std::fs::remove_file(&p);
        data.clear();
    }
}
