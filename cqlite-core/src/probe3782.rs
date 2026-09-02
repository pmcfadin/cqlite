//! TEMPORARY measurement instrumentation for issue #3782. REVERT BEFORE COMMIT.
use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::OnceLock;

fn tally() -> &'static Mutex<BTreeMap<String, usize>> {
    static T: OnceLock<Mutex<BTreeMap<String, usize>>> = OnceLock::new();
    T.get_or_init(|| Mutex::new(BTreeMap::new()))
}

pub fn hit(site: &str, detail: &str) {
    let key = format!("{site} | {detail}");
    if let Ok(mut g) = tally().lock() {
        *g.entry(key).or_insert(0) += 1;
    }
}

pub fn dump(tag: &str) {
    if let Ok(g) = tally().lock() {
        eprintln!("PROBE3782[{tag}] distinct={}", g.len());
        for (k, v) in g.iter() {
            eprintln!("PROBE3782[{tag}] {v:>6}  {k}");
        }
    }
}

pub fn reset() {
    if let Ok(mut g) = tally().lock() {
        g.clear();
    }
}
