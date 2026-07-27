//! Saturation instrumentation for the Flight server (issue #2419, WS2 of epic
//! #2313).
//!
//! Makes the OS-level resources that bind first under concurrent readers legible
//! on the server's own metric surface, so the read-throughput saturation ramp
//! can attribute a plateau to the resource that saturates (the research ranks the
//! order-of-failure as thread/scheduler collapse → queueing → fd exhaustion →
//! memory). Two mechanisms:
//!
//! * **`/proc`-derived process gauges** — [`read_proc_threads`], [`read_proc_fds`],
//!   [`read_proc_rss_bytes`] are pure `Option`-returning `std::fs` reads over
//!   `/proc/self/*` (Linux only; `None` on any non-`/proc` platform), driven by a
//!   single background [`run_sampler`] task on a ~2s cadence. A reader that
//!   returns `None` means the sampler emits NO sample for that gauge — the gauge
//!   is ABSENT from the exposition, never a fabricated `0` (the telemetry
//!   authoritative-data rule, #2314).
//! * **A flight blocking-task gauge** — [`BlockingTaskGuard`] is an RAII guard
//!   incremented on entry to a flight `spawn_blocking` closure and decremented on
//!   exit (incl. panic/cancel), backing `cqlite.flight.blocking_tasks_in_use`. An
//!   honest, dependency-free proxy for blocking-pool pressure — FLIGHT-managed
//!   tasks in flight, NOT the global `tokio` blocking-pool queue depth (which
//!   needs `tokio_unstable`; out of scope, design open fork O1).
//!
//! No new dependencies: RSS is read from the `VmRSS` text field of
//! `/proc/self/status` (no page-size math), and thread/fd counts are directory
//! entry counts. All gauges route through `cqlite_core::observability`, a no-op
//! when its `observability` feature is off, so this compiles and runs identically
//! in every build.

use std::future::Future;
use std::path::Path;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Once;
use std::time::Duration;

use cqlite_core::observability::{self as obs, catalog};

/// Background saturation sampler cadence. A fixed ~2s constant (design fork O4:
/// one fewer tunable for 0.15) — bounded per-tick cost (three small `/proc`
/// reads), chosen over on-demand collection so a wedged `do_get` (no RPC
/// completion, no batch) keeps its thread/fd/RSS footprint visible while it hangs.
pub const DEFAULT_SAMPLE_INTERVAL: Duration = Duration::from_secs(2);

// --- /proc-derived process-resource readers --------------------------------
//
// Each is a pure function over `/proc/self/*`, returning `Some(v)` on Linux and
// `None` on any platform without `/proc`. Deterministic (no wall-clock wait): the
// calling process always has ≥1 thread, several open fds, and a non-zero RSS.

/// Count the entries in a `/proc/self/*` directory (`task` or `fd`), returning
/// `None` if the directory cannot be read. `std::fs::read_dir` excludes `.`/`..`,
/// so the count is the true number of tasks / descriptors. Reading `/proc/self/fd`
/// itself holds one transient descriptor, which is legitimately included in the
/// live reading.
#[cfg(target_os = "linux")]
fn count_dir_entries(path: &str) -> Option<u64> {
    let mut n: u64 = 0;
    for entry in std::fs::read_dir(path).ok()? {
        if entry.is_ok() {
            n = n.saturating_add(1);
        }
    }
    Some(n)
}

/// Process thread count from `/proc/self/task` (Linux). `None` off-`/proc`.
#[cfg(target_os = "linux")]
pub fn read_proc_threads() -> Option<u64> {
    count_dir_entries("/proc/self/task")
}

/// Off-`/proc` platforms: report absence, never a fabricated `0`.
#[cfg(not(target_os = "linux"))]
pub fn read_proc_threads() -> Option<u64> {
    None
}

/// Open file-descriptor count from `/proc/self/fd` (Linux). `None` off-`/proc`.
#[cfg(target_os = "linux")]
pub fn read_proc_fds() -> Option<u64> {
    count_dir_entries("/proc/self/fd")
}

/// Off-`/proc` platforms: report absence, never a fabricated `0`.
#[cfg(not(target_os = "linux"))]
pub fn read_proc_fds() -> Option<u64> {
    None
}

/// Resident set size in BYTES from the `VmRSS` field of `/proc/self/status`
/// (Linux) — a plain-text `kB` field scaled to bytes, dependency-free (no
/// `sysconf` page-size math). `None` off-`/proc` or if the field is absent.
#[cfg(target_os = "linux")]
pub fn read_proc_rss_bytes() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            // Format: `VmRSS:\t     1234 kB` — first whitespace-separated token
            // after the label is the value in kB.
            let kb: u64 = rest.split_whitespace().next()?.parse().ok()?;
            return Some(kb.saturating_mul(1024));
        }
    }
    None
}

/// Off-`/proc` platforms: report absence, never a fabricated `0`.
#[cfg(not(target_os = "linux"))]
pub fn read_proc_rss_bytes() -> Option<u64> {
    None
}

/// Clamp a `u64` reading into the gauge's `i64` domain without a panicking cast
/// (saturates at `i64::MAX`, unreachable for a real thread/fd/RSS reading).
fn as_gauge(v: u64) -> i64 {
    i64::try_from(v).unwrap_or(i64::MAX)
}

// --- tables_discovered: readdir-only table-dir walk (issue #2684) ----------
//
// Counts the `<keyspace>/<table[-uuid]>` SSTable directories currently VISIBLE
// under `--data-dir`, using directory reads ONLY — never a stat-for-generation,
// open, or parse — so the cold-start invariant (#2385) holds: sampling produces
// zero `INDEX_PARSES_TOTAL` delta. Classification is STRUCTURAL (directory
// layout: a dir directly containing a `*-Data.db` NAME), never inferred from
// file contents (#28 no-heuristics).

/// The result of one discovery walk: genuine table dirs and the keyspaces that
/// contain at least one of them.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TableDiscovery {
    /// Genuine `<keyspace>/<table>` SSTable directories found across all
    /// keyspaces.
    pub tables: u64,
    /// Keyspace directories that contain at least one counted table dir.
    pub keyspaces: u64,
}

/// Whether a readdir entry is a directory, using the readdir-provided `d_type`
/// (`DirEntry::file_type`) so the walk stays genuinely readdir-only — NO stat
/// syscall on the filesystems that populate `d_type` (the common case). Only when
/// the filesystem reports an `Unknown` file type (or the `file_type` call itself
/// errors) do we fall back to a `Path::is_dir` stat, so correctness holds
/// everywhere while the fast path avoids the per-entry stat.
fn entry_is_dir(entry: &std::fs::DirEntry) -> bool {
    match entry.file_type() {
        Ok(ft) if ft.is_dir() => true,
        // A symlinked dir is INTENTIONALLY not followed/counted (this arm
        // short-circuits before the `Path::is_dir` stat fallback below, which
        // WOULD follow the link): a deliberate safety choice so the walk never
        // descends a symlink into a snapshot/hardlink tree (or an out-of-tree
        // target) and double-counts or loops. Plain files are likewise not dirs.
        Ok(ft) if ft.is_file() || ft.is_symlink() => false,
        // Unknown d_type or a file_type error: fall back to a stat.
        _ => entry.path().is_dir(),
    }
}

/// Whether `dir` DIRECTLY contains a `*-Data.db` entry (readdir name check only
/// — no stat, no open, no generation parse). This is the sole, structural
/// "is a genuine table dir" test (matches the `DirSource` `-Data.db` prior art).
fn dir_has_data_db(dir: &Path) -> bool {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return false;
    };
    for entry in entries.flatten() {
        if entry
            .file_name()
            .to_str()
            .is_some_and(|n| n.ends_with("-Data.db"))
        {
            return true;
        }
    }
    false
}

/// Walk `data_dir` with readdir only and count genuine `<keyspace>/<table>`
/// SSTable directories (issue #2684).
///
/// A keyspace is any directory directly under `data_dir`. Within it, a table dir
/// is any directory whose name is NOT `snapshots`/`backups` that directly
/// contains a `*-Data.db` entry — this covers both `<table>` and
/// `<table>-<uuid>` layouts, counts a UUID-suffixed dir exactly once, and
/// excludes the `snapshots/`/`backups/` subtrees and any non-table entry. A
/// `data_dir` that is empty, unreadable, or points at a directory with no table
/// dirs yields `tables = 0` (an inert/wrong mount reads zero immediately).
pub fn discover_tables(data_dir: &Path) -> TableDiscovery {
    let mut out = TableDiscovery::default();
    let Ok(keyspaces) = std::fs::read_dir(data_dir) else {
        return out;
    };
    for ks in keyspaces.flatten() {
        if !entry_is_dir(&ks) {
            continue;
        }
        let ks_path = ks.path();
        let Ok(tables) = std::fs::read_dir(&ks_path) else {
            continue;
        };
        let mut ks_tables: u64 = 0;
        for tbl in tables.flatten() {
            let name = tbl.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            // Defensive skip of any `snapshots`/`backups` dir sitting directly
            // under the keyspace. In the REAL Cassandra layout these live UNDER
            // the table dir (`<keyspace>/<table>/snapshots/<snap>/*-Data.db`,
            // `<keyspace>/<table>/backups/…`), NOT as keyspace-level siblings, so
            // this name-skip is largely belt-and-braces. The ACTUAL property that
            // keeps a nested snapshot/backup `Data.db` from being counted is
            // NON-RECURSION: the walk descends exactly two levels (keyspace →
            // table) and `dir_has_data_db` inspects only a dir's DIRECT children,
            // so a `<table>/snapshots/<snap>/*-Data.db` is never reached — the
            // table dir is counted once for its own live `*-Data.db` and the
            // nested snapshot tree is invisible to the walk.
            if name == "snapshots" || name == "backups" {
                continue;
            }
            if !entry_is_dir(&tbl) {
                continue;
            }
            let tbl_path = tbl.path();
            if dir_has_data_db(&tbl_path) {
                ks_tables = ks_tables.saturating_add(1);
            }
        }
        if ks_tables > 0 {
            out.tables = out.tables.saturating_add(ks_tables);
            out.keyspaces = out.keyspaces.saturating_add(1);
        }
    }
    out
}

/// Log the discovered table/keyspace count EXACTLY ONCE, after the first sample
/// (issue #2684) — so an inert/empty mount (`discovered 0 tables ...`) is visible
/// in logs even without a metrics stack (same spirit as the #2128 OTel-inert
/// warn). `Once`-guarded, co-located with the sampler like
/// [`log_platform_support_once`].
fn log_tables_discovered_once(discovery: &TableDiscovery, data_dir: &Path) {
    static LOGGED: Once = Once::new();
    LOGGED.call_once(|| {
        tracing::info!(
            "discovered {} tables across {} keyspaces under {}",
            discovery.tables,
            discovery.keyspaces,
            data_dir.display()
        );
    });
}

// --- Flight blocking-task gauge --------------------------------------------

/// Process-wide count of flight-managed `spawn_blocking` tasks currently
/// outstanding, backing `cqlite.flight.blocking_tasks_in_use`. A single shared
/// atomic (not thread-local) so the increment (closure entry) and decrement
/// (closure exit, on any path) stay correct even though the closure runs on a
/// blocking-pool thread distinct from the spawner.
static BLOCKING_TASKS: AtomicI64 = AtomicI64::new(0);

fn record_blocking(level: i64) {
    // Floor at 0 so an unexpected imbalance never records a negative gauge
    // (matches `RpcMetrics::finish`).
    obs::record_gauge(catalog::FLIGHT_BLOCKING_TASKS_IN_USE, level.max(0), &[]);
}

/// Apply `delta` to `atomic` and return the resulting level — the exact
/// arithmetic [`BlockingTaskGuard`] applies to the shared [`BLOCKING_TASKS`].
/// Parameterized over the atomic (issue #2419 roborev job 1734, the #2451
/// flake class — mirrors `channel_depth::adjust`) so a test can pin the guard's
/// rise/balance/panic-decrements behavior against a private, per-test atomic
/// instead of racing every other concurrently-running test that also holds a
/// REAL guard against the shared global (e.g. the streaming e2e wiring test).
fn adjust(atomic: &AtomicI64, delta: i64) -> i64 {
    atomic.fetch_add(delta, Ordering::SeqCst) + delta
}

/// RAII guard that accounts one flight `spawn_blocking` task as in-use for its
/// lifetime. Constructed as the FIRST act inside a flight `spawn_blocking`
/// closure ([`crate::streaming`]); its `Drop` decrements on EVERY exit path —
/// normal return, early `?`, cancel, or panic — so the increment/decrement are
/// balanced by construction (mirroring the #2316 `ProducerThreadGuard`).
pub(crate) struct BlockingTaskGuard {
    /// The atomic this guard tracks — always the shared [`BLOCKING_TASKS`] in
    /// production ([`Self::enter`]); a test-only constructor
    /// ([`Self::enter_on`]) can point it at a private, per-test atomic.
    atomic: &'static AtomicI64,
    /// Whether to emit the real OTel gauge on change. `false` for a
    /// test-injected atomic, so a unit test pinning the guard's arithmetic
    /// against a private atomic never publishes a synthetic reading over
    /// `cqlite.flight.blocking_tasks_in_use`.
    emit: bool,
    /// The level [`Self::atomic`] held immediately AFTER this guard's own
    /// increment — i.e. a value that provably includes this guard's `+1`
    /// (issue #2896). Retained (it is already computed by [`adjust`]) so the
    /// end-to-end streaming wiring test can assert on an observation
    /// attributable to ITS OWN guard, instead of differencing the shared global
    /// against a baseline snapshot that concurrently-running peer tests can
    /// inflate and then deflate.
    entry_level: i64,
}

impl BlockingTaskGuard {
    /// Enter a flight blocking task: increment the in-use gauge and return the
    /// guard whose drop decrements it.
    pub(crate) fn enter() -> Self {
        let entry_level = adjust(&BLOCKING_TASKS, 1);
        record_blocking(entry_level);
        Self {
            atomic: &BLOCKING_TASKS,
            emit: true,
            entry_level,
        }
    }

    /// The shared in-use level observed at this guard's OWN entry — the exact
    /// post-increment value [`Self::enter`] published to the gauge, so it is
    /// `>= 1` by construction and can never be invalidated by another guard
    /// dropping afterwards (issue #2896). Published through
    /// `crate::streaming::StreamProbe` so the streaming wiring test observes the
    /// production arithmetic on the REAL shared atomic.
    pub(crate) fn entry_level(&self) -> i64 {
        self.entry_level
    }

    /// Test-only: enter a guard tracking `atomic` instead of the shared
    /// [`BLOCKING_TASKS`] (issue #2419 roborev job 1734). `atomic` must be
    /// `'static` — a test declares a function-local `static` (no heap leak
    /// needed) so each test gets its own private counter, immune to
    /// concurrently-running tests that hold a REAL guard against the shared
    /// global. Never emits the OTel gauge (the level is synthetic, not a real
    /// blocking-pool reading).
    #[cfg(test)]
    fn enter_on(atomic: &'static AtomicI64) -> Self {
        let entry_level = adjust(atomic, 1);
        Self {
            atomic,
            emit: false,
            entry_level,
        }
    }
}

impl Drop for BlockingTaskGuard {
    fn drop(&mut self) {
        let level = adjust(self.atomic, -1);
        if self.emit {
            record_blocking(level);
        }
    }
}

/// Read the current process-wide flight blocking-task in-use level (issue #2419).
///
/// Exposes the same atomic that drives `cqlite.flight.blocking_tasks_in_use`, so
/// an end-to-end streaming test can assert the level rises while blocking tasks
/// are outstanding and returns to its pre-load baseline after every task exits
/// (asserting on the LEVEL, never on timing). Feature-independent (the atomic is
/// maintained regardless of the `observability` OTel feature; only the emission
/// is gated), mirroring [`crate::obs::in_flight_level`].
pub fn blocking_tasks_in_use_level() -> i64 {
    BLOCKING_TASKS.load(Ordering::SeqCst)
}

// --- Background sampler -----------------------------------------------------

/// Total collection ticks the sampler has performed (a `do_get`-independent
/// work-probe): incremented once per `sample_once`, whether or not any `/proc`
/// reader returned `Some`, so it confirms the sampler ran even on a non-`/proc`
/// platform.
static SAMPLE_TICKS: AtomicU64 = AtomicU64::new(0);

/// Read the total number of sampler collection ticks performed (issue #2419) —
/// the sampler's work-probe. `#[cfg(test)]`-only: consumed solely by the
/// in-crate sampler tests (no production reader), so it would otherwise be
/// flagged dead code under `-D warnings`.
#[cfg(test)]
pub(crate) fn sample_ticks() -> u64 {
    SAMPLE_TICKS.load(Ordering::SeqCst)
}

/// Perform one collection tick: read each `/proc` gauge and record ONLY the ones
/// that returned `Some` (a `None` reader emits no sample — absence, never `0`),
/// then walk `data_dir` (readdir only, issue #2684) and record
/// `cqlite.flight.tables_discovered`. Returns the discovery result so the caller
/// can emit the one-time startup log line after the first sample.
///
/// The `data_dir` walk (a `readdir` tree-scan whose cost scales with keyspaces ×
/// tables, and which can block on a slow/network-backed mount) is offloaded to a
/// `tokio::task::spawn_blocking` pool thread so it NEVER stalls a runtime worker
/// polling in-flight `do_get` scan futures (roborev, issue #2684). If that
/// blocking task fails to join (e.g. runtime shutdown), this tick simply skips the
/// `tables_discovered` emission and returns an empty discovery — never a panic.
///
/// Unlike the `/proc` gauges, `tables_discovered` is emitted UNCONDITIONALLY
/// (including `0`): a `0` here is an authoritative reading of an empty/wrong
/// mount, NOT the "absence" a `None` /proc reader represents — surfacing an inert
/// mount is the whole point.
async fn sample_once(data_dir: &Path) -> TableDiscovery {
    if let Some(threads) = read_proc_threads() {
        obs::record_gauge(catalog::PROC_THREADS, as_gauge(threads), &[]);
    }
    if let Some(fds) = read_proc_fds() {
        obs::record_gauge(catalog::PROC_FDS, as_gauge(fds), &[]);
    }
    if let Some(rss) = read_proc_rss_bytes() {
        obs::record_gauge(catalog::PROC_RSS_BYTES, as_gauge(rss), &[]);
    }
    // Every tick counts as a completed collection (the work-probe), independent of
    // the offloaded walk's outcome.
    SAMPLE_TICKS.fetch_add(1, Ordering::SeqCst);
    // Run the blocking readdir tree-scan OFF the async executor.
    let dir = data_dir.to_path_buf();
    let discovery = match tokio::task::spawn_blocking(move || discover_tables(&dir)).await {
        Ok(discovery) => discovery,
        Err(_join_err) => {
            // The blocking task failed to join (runtime teardown); skip this
            // tick's emission rather than panicking.
            return TableDiscovery::default();
        }
    };
    obs::record_gauge(
        catalog::FLIGHT_TABLES_DISCOVERED,
        as_gauge(discovery.tables),
        &[],
    );
    discovery
}

/// Log the platform's `/proc` support state EXACTLY ONCE (design D2), so an
/// operator on a non-`/proc` platform learns the `cqlite.proc.*` gauges will be
/// absent (never per-sample spam).
fn log_platform_support_once() {
    static LOGGED: Once = Once::new();
    LOGGED.call_once(|| {
        if read_proc_threads().is_none() {
            tracing::info!(
                "saturation sampler: /proc is unavailable on this platform; the \
                 cqlite.proc.threads/fds/rss_bytes gauges will be ABSENT (no \
                 fabricated zero). The server starts and serves normally."
            );
        } else {
            tracing::debug!("saturation sampler started; cqlite.proc.* gauges active");
        }
    });
}

/// Run the background saturation sampler until `shutdown` resolves.
///
/// Takes one immediate startup sample (so the `cqlite.proc.*` gauges are visible
/// the moment the server starts, before any load) and then ticks every
/// `interval` (production: [`DEFAULT_SAMPLE_INTERVAL`]), calling [`sample_once`]
/// on each tick. Returns promptly when `shutdown` resolves — no leaked task, no
/// busy-spin (a `tokio::select!` between the interval and the shutdown future).
/// Spawned at server startup and wired to the same shutdown source as the tonic
/// server. Because the initial sample runs before the select loop, the sampler
/// always performs at least one collection tick even if shutdown is already
/// pending (deterministic, no reliance on interval first-tick timing).
pub async fn run_sampler<S>(interval: Duration, data_dir: std::path::PathBuf, shutdown: S)
where
    S: Future<Output = ()>,
{
    log_platform_support_once();
    tokio::pin!(shutdown);
    // Immediate startup sample; the periodic ticker then fires after each full
    // `interval` (never an extra immediate tick — `interval_at` from `now +
    // interval` — so the cadence stays regular).
    let first = sample_once(&data_dir).await;
    // One-time startup log line (issue #2684) after the first walk, so an
    // empty/wrong mount is visible in logs even without a metrics stack.
    log_tables_discovered_once(&first, &data_dir);
    let start = tokio::time::Instant::now() + interval;
    let mut ticker = tokio::time::interval_at(start, interval);
    loop {
        tokio::select! {
            _ = ticker.tick() => {
                sample_once(&data_dir).await;
            }
            _ = &mut shutdown => break,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Stage 1.2: on Linux each `/proc` reader returns `Some(v)` with `v > 0` (a
    /// live, deterministic self-read); on a non-`/proc` platform each returns
    /// `None` (the absence branch — no fabricated `0`). Whichever branch this
    /// build compiles is exercised.
    #[test]
    fn proc_readers_match_platform() {
        #[cfg(target_os = "linux")]
        {
            assert!(
                read_proc_threads().is_some_and(|v| v > 0),
                "Linux: the calling process has ≥1 thread"
            );
            assert!(
                read_proc_fds().is_some_and(|v| v > 0),
                "Linux: the calling process has ≥1 open fd"
            );
            assert!(
                read_proc_rss_bytes().is_some_and(|v| v > 0),
                "Linux: the calling process has a non-zero resident set"
            );
        }
        #[cfg(not(target_os = "linux"))]
        {
            assert_eq!(read_proc_threads(), None, "off-/proc: absence, not 0");
            assert_eq!(read_proc_fds(), None, "off-/proc: absence, not 0");
            assert_eq!(read_proc_rss_bytes(), None, "off-/proc: absence, not 0");
        }
    }

    /// Spec Requirement 1 scenario ("Thread and fd gauges rise with concurrent
    /// scans and settle after"): the process thread count read WHILE extra
    /// threads are parked is strictly greater than the pre-load baseline, and
    /// after they all exit it drops back below the loaded peak — asserted by
    /// comparing captured LEVEL snapshots, never by asserting elapsed time. Not
    /// `#[cfg]`-gated (so it compiles + clippy-checks on every platform); it
    /// runs the real assertions only where `/proc` is present and early-returns
    /// on an off-`/proc` platform, whose absence semantics `proc_readers_match_platform`
    /// already covers.
    #[test]
    fn proc_thread_gauge_rises_with_load_and_settles() {
        let Some(base) = read_proc_threads() else {
            // Off-/proc platform: readers report absence (covered elsewhere).
            return;
        };
        let n = 8usize;
        // A barrier so every spawned thread is simultaneously alive when we read
        // the loaded snapshot, and a second so they exit only after we have.
        let started = std::sync::Arc::new(std::sync::Barrier::new(n + 1));
        let release = std::sync::Arc::new(std::sync::Barrier::new(n + 1));
        let handles: Vec<_> = (0..n)
            .map(|_| {
                let s = started.clone();
                let r = release.clone();
                std::thread::spawn(move || {
                    s.wait();
                    r.wait();
                })
            })
            .collect();

        started.wait(); // all n threads are now alive and parked
        let loaded = read_proc_threads().expect("linux self-read while loaded");
        assert!(
            loaded > base,
            "the thread count must rise while {n} extra threads are parked \
             (base={base}, loaded={loaded})"
        );

        release.wait(); // let the parked threads finish
        for h in handles {
            let _ = h.join();
        }

        // Settle: after every spawned thread has exited, the count returns below
        // the loaded peak. Thread-table teardown in /proc can lag the join, so
        // read the LEVEL over a bounded number of probes (a bounded work-probe,
        // not a fixed wall-clock sleep).
        let mut settled = read_proc_threads().expect("linux self-read after join");
        for _ in 0..1000 {
            if settled < loaded {
                break;
            }
            std::thread::yield_now();
            settled = read_proc_threads().expect("linux self-read after join");
        }
        assert!(
            settled < loaded,
            "the released threads must drop the count back below the loaded peak \
             (loaded={loaded}, settled={settled})"
        );
    }

    /// Stage 1.2 corollary: a `None` reader contributes NO sample to a tick, so
    /// the gauge is absent rather than `0`. Exercised by driving `sample_once`
    /// and confirming it never panics and always advances the tick probe,
    /// regardless of platform (on non-`/proc` platforms all three readers are
    /// `None` and no gauge is recorded, yet the tick still counts).
    #[tokio::test]
    async fn sample_once_advances_tick_and_skips_none_readers() {
        let before = sample_ticks();
        let tmp = tempfile::TempDir::new().expect("tempdir");
        sample_once(tmp.path()).await;
        assert!(
            sample_ticks() > before,
            "a collection tick is counted even when every /proc reader is None"
        );
    }

    /// Stage 2.2: the blocking-task gauge RISES with concurrent guards and
    /// balances back to baseline on every exit path (RAII drop), asserted on the
    /// LEVEL, not on timing.
    ///
    /// Roborev job 1734 (the #2451 flake class): pinned against a PRIVATE,
    /// per-test `static` atomic via [`BlockingTaskGuard::enter_on`] — never the
    /// shared [`BLOCKING_TASKS`] global — because
    /// `blocking_tasks_gauge_tracks_real_streaming_do_get` (the streaming e2e
    /// wiring test, same binary, parallel runner) holds a REAL guard against
    /// that shared global concurrently, which would flake this test's
    /// exact-equality assertions exactly like the #2419 egress-depth fix.
    #[test]
    fn blocking_task_guard_rises_and_balances() {
        static LOCAL: AtomicI64 = AtomicI64::new(0);
        let base = LOCAL.load(Ordering::SeqCst);
        {
            let _g1 = BlockingTaskGuard::enter_on(&LOCAL);
            assert_eq!(LOCAL.load(Ordering::SeqCst), base + 1);
            let _g2 = BlockingTaskGuard::enter_on(&LOCAL);
            assert_eq!(
                LOCAL.load(Ordering::SeqCst),
                base + 2,
                "a second concurrent blocking task must ADD to the in-use count"
            );
        }
        assert_eq!(
            LOCAL.load(Ordering::SeqCst),
            base,
            "every guard's drop decrements — the level returns to its baseline"
        );
    }

    /// A guard dropped on the panic-unwind path still decrements (RAII), so a
    /// panicking blocking closure never leaks in-use count. Asserted on the
    /// level, pinned against a private per-test atomic for the same
    /// determinism reason as the test above (roborev job 1734).
    #[test]
    fn blocking_task_guard_decrements_on_panic() {
        static LOCAL: AtomicI64 = AtomicI64::new(0);
        let base = LOCAL.load(Ordering::SeqCst);
        let result = std::panic::catch_unwind(|| {
            let _g = BlockingTaskGuard::enter_on(&LOCAL);
            panic!("simulated blocking-closure panic");
        });
        assert!(result.is_err(), "the closure panicked as set up");
        assert_eq!(
            LOCAL.load(Ordering::SeqCst),
            base,
            "the guard's Drop ran during unwind, restoring the baseline"
        );
    }

    /// Build a `<data_dir>/<keyspace>/<table>/` dir and (optionally) drop a
    /// `*-Data.db` name into it. `data_only=false` makes a table dir WITHOUT any
    /// Data.db (a non-table entry that must NOT be counted).
    fn make_table_dir(data_dir: &Path, keyspace: &str, table: &str, with_data: bool) {
        let dir = data_dir.join(keyspace).join(table);
        std::fs::create_dir_all(&dir).expect("create table dir");
        if with_data {
            std::fs::write(dir.join("nb-1-big-Data.db"), b"x").expect("write data.db");
        }
    }

    /// Issue #2684 spec Requirement 2, modeling the REAL on-disk layout: a
    /// UUID-suffixed table dir counts EXACTLY ONCE even though it also contains
    /// the genuine Cassandra `snapshots/`/`backups/` subtrees (which, in real
    /// Cassandra, live UNDER the table dir — `<keyspace>/<table>/snapshots/<snap>/`
    /// — as hardlinks to the live SSTables, NOT as keyspace-level siblings). The
    /// nested snapshot/backup `Data.db` hardlinks must NOT be double-counted; the
    /// non-recursive two-level walk (keyspace → table, direct-children `Data.db`
    /// check) is what guarantees this. A dir with no direct `Data.db` is excluded.
    #[test]
    fn discover_counts_genuine_table_dirs_only() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let root = tmp.path();
        // A UUID-suffixed table dir with a live Data.db — the genuine table.
        let table = "users-8f3a9c2e11114b0d9c7e2a1b3d4f5e6a";
        make_table_dir(root, "ks", table, true);
        let table_dir = root.join("ks").join(table);

        // REAL LAYOUT: snapshots/ and backups/ live UNDER the table dir and hold
        // hardlinked *-Data.db copies of the live SSTable. These nested Data.db
        // files must NOT be counted (they are not the table's own live children,
        // and the non-recursive walk never descends into them).
        let snap = table_dir.join("snapshots").join("snap1");
        std::fs::create_dir_all(&snap).expect("nested snapshots dir");
        std::fs::write(snap.join("nb-1-big-Data.db"), b"x").expect("snap data");
        let backup = table_dir.join("backups").join("bkp1");
        std::fs::create_dir_all(&backup).expect("nested backups dir");
        std::fs::write(backup.join("nb-1-big-Data.db"), b"x").expect("backup data");

        // A directory with NO Data.db — a non-table entry, excluded.
        make_table_dir(root, "ks", "not_a_table", false);
        // A stray plain file directly under the keyspace — excluded (not a dir).
        std::fs::write(root.join("ks").join("README.txt"), b"x").expect("stray file");

        let d = discover_tables(root);
        assert_eq!(
            d.tables, 1,
            "the users table counts EXACTLY once — its nested snapshots/ and \
             backups/ Data.db hardlinks are not double-counted (non-recursion), \
             and the no-Data.db dir is excluded"
        );
        assert_eq!(d.keyspaces, 1, "one keyspace contains a genuine table");
    }

    /// Issue #2684 spec Requirement 1: bidirectional — remove a table dir and the
    /// count falls; add one and it rises; an empty dir reads 0.
    #[test]
    fn discover_is_bidirectional_and_zero_on_empty() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let root = tmp.path();
        assert_eq!(
            discover_tables(root).tables,
            0,
            "an empty/wrong mount reads zero immediately"
        );

        make_table_dir(root, "ks1", "a", true);
        make_table_dir(root, "ks1", "b", true);
        make_table_dir(root, "ks2", "c", true);
        let d = discover_tables(root);
        assert_eq!(d.tables, 3, "three genuine table dirs across two keyspaces");
        assert_eq!(d.keyspaces, 2);

        // Remove one table dir → the count falls.
        std::fs::remove_dir_all(root.join("ks1").join("b")).expect("remove table dir");
        assert_eq!(
            discover_tables(root).tables,
            2,
            "removing a table dir lowers the discovered count"
        );

        // Add a new table dir → the count rises.
        make_table_dir(root, "ks2", "d", true);
        assert_eq!(
            discover_tables(root).tables,
            3,
            "a newly-appeared table dir raises the discovered count"
        );
    }

    /// Issue #2684 spec Requirement 3 (cold-start invariant #2385): a full
    /// `sample_once` walk over a populated dir performs no SSTable open/parse —
    /// exercised here as a smoke test (the OTel `INDEX_PARSES_TOTAL` zero-delta
    /// assertion lives in the observability-testing capture harness). Confirms
    /// the walk sees the fixture and the tick still advances.
    #[tokio::test]
    async fn sample_once_walks_data_dir_without_panicking() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        make_table_dir(tmp.path(), "ks", "t", true);
        let before = sample_ticks();
        let d = sample_once(tmp.path()).await;
        assert_eq!(d.tables, 1, "sample_once walks the data dir and counts");
        assert!(sample_ticks() > before, "the tick advances");
    }

    /// Stage 3.2: the sampler performs ≥1 collection tick and its handle RESOLVES
    /// after the shutdown signal (it does not run forever, does not busy-spin) —
    /// asserted on task completion, never a wall-clock sleep.
    #[tokio::test]
    async fn sampler_ticks_at_least_once_then_stops_on_shutdown() {
        let base = sample_ticks();
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let data_dir = tmp.path().to_path_buf();
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let handle = tokio::spawn(run_sampler(
            Duration::from_millis(5),
            data_dir,
            async move {
                let _ = rx.await;
            },
        ));

        // Signal shutdown; `run_sampler`'s unconditional pre-loop `sample_once()`
        // (not an interval-tick race) guarantees ≥1 collection has already
        // happened before the `shutdown` future is even polled, regardless of
        // how quickly it resolves.
        let _ = tx.send(());

        // Assert on COMPLETION (the handle resolving), with a generous safety
        // timeout — not a fixed sleep. A sampler that ran forever would time out.
        let joined = tokio::time::timeout(Duration::from_secs(5), handle).await;
        let task_result =
            joined.expect("the sampler handle must resolve after shutdown (no forever-run)");
        task_result.expect("the sampler task completed without panicking");
        assert!(
            sample_ticks() > base,
            "the sampler performed at least one collection tick before stopping"
        );
    }
}
