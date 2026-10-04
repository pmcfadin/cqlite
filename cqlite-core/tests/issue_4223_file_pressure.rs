//! FD pressure must produce a typed error through both manager constructors.
//! Resource limits are changed only in a subprocess, never in the test runner.
#![cfg(unix)]

#[allow(dead_code)]
#[path = "support/datasets_root.rs"]
mod datasets_root;

use cqlite_core::storage::sstable::SSTableManager;
use cqlite_core::{Config, Error, Platform};
use std::{fs::File, process::Command, sync::Arc};

#[test]
fn manager_open_under_fd_pressure_returns_io_error() {
    for constructor in ["base", "discovered"] {
        let output = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "file_pressure_child", "--nocapture"])
            .env("CQLITE_4223_CHILD", constructor)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{constructor}: {}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains("typed EMFILE observed"));
    }
}

#[test]
fn file_pressure_child() {
    let Ok(constructor) = std::env::var("CQLITE_4223_CHILD") else {
        return;
    };
    let table = datasets_root::resolve_table_generation_dir("test_basic", "simple_table")
        .expect("real Cassandra simple_table fixture required");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let mut config = Config::default();
        // mmap can open and release each descriptor in turn, legitimately
        // succeeding with one spare slot. Buffered readers retain their FDs.
        config.storage.disk_access_mode = cqlite_core::config::DiskAccessMode::Buffered;
        let platform = Arc::new(Platform::new(&config).await.unwrap());
        let healthy = SSTableManager::new(
            &table,
            &config,
            platform.clone(),
            #[cfg(feature = "state_machine")]
            None,
        )
        .await
        .unwrap();
        assert!(healthy.stats().await.unwrap().sstable_count > 0);
        drop(healthy);
        let mut previous = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        // SAFETY: `previous` is a valid writable rlimit. This child has its own
        // descriptor table; the parent runner's limits and descriptors cannot change.
        assert_eq!(
            unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut previous) },
            0
        );
        let limited = libc::rlimit {
            rlim_cur: previous.rlim_cur.min(256),
            rlim_max: previous.rlim_max,
        };
        // SAFETY: valid rlimit pointer; only lower the soft limit in this child.
        assert_eq!(unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &limited) }, 0);
        let mut held = Vec::new();
        loop {
            match File::open("/dev/null") {
                Ok(file) => held.push(file),
                Err(error) => {
                    assert_eq!(error.raw_os_error(), Some(libc::EMFILE));
                    break;
                }
            }
        }
        // Directory enumeration can acquire one descriptor; the subsequent
        // SSTable component open must encounter the real OS limit.
        held.pop();
        let result = if constructor == "discovered" {
            SSTableManager::new_from_discovered_paths(
                &table,
                vec![table.clone()],
                &config,
                platform,
                #[cfg(feature = "state_machine")]
                None,
            )
            .await
        } else {
            SSTableManager::new(
                &table,
                &config,
                platform,
                #[cfg(feature = "state_machine")]
                None,
            )
            .await
        };
        drop(held);
        // SAFETY: restore the exact limits read above before assertions print.
        assert_eq!(
            unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &previous) },
            0
        );
        match result {
            Err(Error::Io(error)) if error.raw_os_error() == Some(libc::EMFILE) => {
                println!("typed EMFILE observed");
            }
            Err(error) => panic!("expected typed EMFILE, got {error:?}"),
            Ok(manager) => panic!(
                "expected typed EMFILE, got a successful manager with {} readers",
                manager.stats().await.unwrap().sstable_count
            ),
        }
    });
}
