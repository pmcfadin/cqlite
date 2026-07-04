//! Retry-policy guards for the block I/O layer ([`super`] = `block_io`): a
//! transient (EINTR-class) fault triggers EXACTLY one retry that re-seeks back
//! to the original offset, while deterministic corruption and non-transient I/O
//! are never retried (issue #1588).
//!
//! Split out of `block_io.rs` to keep that source file under the campsite-rule
//! size limit (issue #1135). Included via
//! `#[cfg(test)] #[path = "block_io_retry_tests.rs"] mod retry_tests;` in the
//! parent, so `use super::*` resolves to `block_io`'s private items
//! (`retry_transient_once`, `is_transient_io`, `BlockSource`, `Error`, …).

use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use tempfile::TempDir;

/// Build an `Arc<Mutex<BlockSource>>` over `bytes`. The returned `TempDir` MUST
/// be held for the source's lifetime. (Local to this module so the retry guards
/// are self-contained, mirroring the sibling-test-file pattern.)
async fn blocksource_from(bytes: &[u8]) -> (TempDir, Arc<Mutex<BlockSource>>) {
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().join("data.bin");
    tokio::fs::write(&path, bytes).await.expect("write data");
    let file = tokio::fs::File::open(&path).await.expect("open data");
    (dir, Arc::new(Mutex::new(BlockSource::buffered(file))))
}

fn transient_io() -> Error {
    Error::Io(std::io::Error::new(
        std::io::ErrorKind::Interrupted,
        "simulated EINTR",
    ))
}

/// A transient (EINTR-class) fault triggers EXACTLY one retry, and that retry
/// re-reads the SAME (original) offset — proving the source is re-seeked back
/// after the first attempt advanced the position. This is the moved-position
/// bug: without the re-seek the second attempt would observe offset 8.
#[tokio::test]
async fn retry_transient_reseeks_to_original_offset() {
    let (_dir, file) = blocksource_from(&[0u8; 16]).await;
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = Arc::new(Mutex::new(Vec::<u64>::new()));

    let f = file.clone();
    let c = calls.clone();
    let s = seen.clone();
    let attempt = move || {
        let f = f.clone();
        let c = c.clone();
        let s = s.clone();
        async move {
            let n = c.fetch_add(1, Ordering::Relaxed);
            let pos = { f.lock().await.stream_position().await.unwrap() };
            s.lock().await.push(pos);
            if n == 0 {
                // Simulate a partial read that advanced the position, then a
                // transient fault mid-block.
                {
                    let mut g = f.lock().await;
                    g.seek(std::io::SeekFrom::Start(8)).await.unwrap();
                }
                Err(transient_io())
            } else {
                Ok::<u32, Error>(99)
            }
        }
    };

    let out = retry_transient_once(&file, attempt).await.unwrap();
    assert_eq!(out, 99);
    assert_eq!(
        calls.load(Ordering::Relaxed),
        2,
        "exactly one transient retry (two attempts total)"
    );
    assert_eq!(
        seen.lock().await.clone(),
        vec![0, 0],
        "retry must re-read the SAME original offset, not the moved position"
    );
}

/// A deterministic corruption error is NOT retried and does NOT sleep: exactly
/// one attempt, returns fast. The attempt counter is the robust proof (no
/// wall-clock race); the elapsed bound is a generous secondary guard.
#[tokio::test]
async fn retry_does_not_retry_or_sleep_on_deterministic_corruption() {
    let (_dir, file) = blocksource_from(&[0u8; 16]).await;
    let calls = Arc::new(AtomicUsize::new(0));

    let c = calls.clone();
    let attempt = move || {
        let c = c.clone();
        async move {
            c.fetch_add(1, Ordering::Relaxed);
            Err::<u32, Error>(Error::corruption("CRC32 mismatch"))
        }
    };

    let start = std::time::Instant::now();
    let err = retry_transient_once(&file, attempt).await.unwrap_err();
    let elapsed = start.elapsed();

    assert!(
        matches!(err, Error::Corruption(_)),
        "typed corruption: {err}"
    );
    assert_eq!(
        calls.load(Ordering::Relaxed),
        1,
        "corruption is deterministic — never retried"
    );
    assert!(
        elapsed < std::time::Duration::from_millis(500),
        "no sleep on the deterministic-error path: {elapsed:?}"
    );
}

/// A NON-transient I/O error (e.g. NotFound) is not retried either: only
/// EINTR-class transient faults qualify, unlike the broad `is_recoverable`
/// classification which treats every `Io` as retryable.
#[tokio::test]
async fn retry_does_not_retry_non_transient_io() {
    let (_dir, file) = blocksource_from(&[0u8; 16]).await;
    let calls = Arc::new(AtomicUsize::new(0));

    let c = calls.clone();
    let attempt = move || {
        let c = c.clone();
        async move {
            c.fetch_add(1, Ordering::Relaxed);
            Err::<u32, Error>(Error::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "gone",
            )))
        }
    };

    let err = retry_transient_once(&file, attempt).await.unwrap_err();
    assert!(matches!(err, Error::Io(_)));
    assert_eq!(
        calls.load(Ordering::Relaxed),
        1,
        "non-transient I/O (NotFound) is not retried"
    );
}

#[test]
fn is_transient_io_classifies_only_eintr_class() {
    assert!(is_transient_io(&transient_io()));
    assert!(is_transient_io(&Error::Io(std::io::Error::new(
        std::io::ErrorKind::WouldBlock,
        "eagain"
    ))));
    assert!(is_transient_io(&Error::Io(std::io::Error::new(
        std::io::ErrorKind::TimedOut,
        "etimedout"
    ))));
    // Deterministic / non-transient are NOT transient.
    assert!(!is_transient_io(&Error::corruption("crc")));
    assert!(!is_transient_io(&Error::invalid_format("bad")));
    assert!(!is_transient_io(&Error::Io(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        "nf"
    ))));
}
