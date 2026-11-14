//! Issue #4498, roborev r7: the fault shim's `open*` interposers must count more than `O_CREAT`.
//!
//! * `open`/`openat` with `O_TRUNC` on an EXISTING file opened writable mutates it with no
//!   `O_CREAT`, so it is a faultable truncate-class boundary under the fault root.
//! * `O_TMPFILE` is a create (and reads its mode argument like `O_CREAT`).
//!
//! Every probe is a C program calling libc DIRECTLY under the REAL shim
//! (`tests/support/crash_fault_shim.c`, built with `cc`; fails closed without one). Split out of
//! `issue_4498_crash_consistency.rs` to keep that file under its size threshold.

#![cfg(all(
    feature = "write-support",
    not(feature = "tombstones"),
    target_os = "linux",
    any(target_arch = "x86_64", target_arch = "aarch64")
))]

use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus};
use std::sync::OnceLock;

use tempfile::TempDir;

fn shim() -> &'static Path {
    static SHIM: OnceLock<(TempDir, PathBuf)> = OnceLock::new();
    &SHIM
        .get_or_init(|| {
            let dir = TempDir::new().expect("shim tempdir");
            let so = dir.path().join("crash_fault_shim.so");
            let src =
                PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/support/crash_fault_shim.c");
            let out = Command::new("cc")
                .args(["-shared", "-fPIC", "-O1", "-o"])
                .arg(&so)
                .arg(&src)
                .arg("-ldl")
                .output()
                .unwrap_or_else(|e| panic!("FAIL CLOSED: `cc` could not be run ({e})"));
            assert!(
                out.status.success(),
                "crash_fault_shim.c failed to compile:\n{}",
                String::from_utf8_lossy(&out.stderr)
            );
            (dir, so)
        })
        .1
}

/// Compile `src`, run it under the shim (`fault`: `(boundary number, mode)`), return
/// `(status, boundary log, stderr)`.
fn run_c(
    tag: &str,
    src: &str,
    root: &Path,
    fault: Option<(usize, &str)>,
) -> (ExitStatus, String, String) {
    let tmp = TempDir::new().expect("tempdir");
    let c = tmp.path().join(format!("{tag}.c"));
    let exe = tmp.path().join(tag);
    std::fs::write(&c, src).expect("write c");
    let out = Command::new("cc")
        .arg(&c)
        .arg("-o")
        .arg(&exe)
        .output()
        .unwrap_or_else(|e| panic!("FAIL CLOSED: `cc` could not be run ({e})"));
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let log = tmp.path().join("log");
    let mut cmd = Command::new(&exe);
    cmd.env("LD_PRELOAD", shim())
        .env("CQLITE_FAULT_ROOT", root)
        .env("CQLITE_FAULT_LOG", &log);
    if let Some((at, mode)) = fault {
        cmd.env("CQLITE_FAULT_AT", at.to_string())
            .env("CQLITE_FAULT_MODE", mode);
    }
    let run = cmd.output().expect("run c program");
    (
        run.status,
        std::fs::read_to_string(&log).unwrap_or_default(),
        String::from_utf8_lossy(&run.stderr).into_owned(),
    )
}

/// The 1-based number the shim logged for the first `kind` boundary whose path ends in `tail`.
fn boundary_number(log: &str, kind: &str, tail: &str) -> Option<usize> {
    log.lines().find_map(|l| {
        let mut it = l.splitn(3, ' ');
        let n: usize = it.next()?.parse().ok()?;
        (it.next()? == kind && it.next()?.ends_with(tail)).then_some(n)
    })
}

/// For each of `open`/`open64`/`openat`/`openat64`: `O_TRUNC` on an existing writable file is a
/// logged `truncate` boundary, the data is still destroyed when unfaulted, and a fault AT that
/// boundary leaves the file's bytes untouched (ENOSPC) — i.e. it really can be faulted.
#[test]
fn o_trunc_on_an_existing_file_is_a_faultable_truncate_boundary() {
    for (name, call) in [
        ("open", r#"open(PATH, O_WRONLY | O_TRUNC)"#),
        ("open64", r#"open64(PATH, O_RDWR | O_TRUNC)"#),
        ("openat", r#"openat(AT_FDCWD, PATH, O_WRONLY | O_TRUNC)"#),
        ("openat64", r#"openat64(AT_FDCWD, PATH, O_RDWR | O_TRUNC)"#),
    ] {
        let dir = TempDir::new().expect("tempdir");
        let root = dir.path().canonicalize().unwrap();
        let file = root.join("existing.bin");
        let src = format!(
            r#"#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#define PATH "{path}"
int main(void) {{
  int fd = {call};
  if (fd < 0) return errno == ENOSPC ? 7 : 3;
  close(fd);
  return 0;
}}"#,
            path = file.display()
        );

        // Unfaulted: boundary logged, file truncated.
        std::fs::write(&file, b"precious bytes").expect("seed");
        let (status, log, stderr) = run_c(name, &src, &root, None);
        assert!(status.success(), "{name}: {status:?} {stderr}");
        let n = boundary_number(&log, "truncate", "/existing.bin").unwrap_or_else(|| {
            panic!("{name}: no `truncate` boundary for O_TRUNC on an existing file: {log:?}")
        });
        assert_eq!(
            std::fs::read(&file).unwrap(),
            b"",
            "{name}: O_TRUNC must still truncate"
        );
        assert!(
            boundary_number(&log, "create", "/existing.bin").is_none(),
            "{name}: an existing file is not a create: {log:?}"
        );

        // Faulted at exactly that boundary: the open fails with ENOSPC and the bytes survive.
        std::fs::write(&file, b"precious bytes").expect("re-seed");
        let (status, _, stderr) = run_c(name, &src, &root, Some((n, "enospc")));
        assert_eq!(
            status.code(),
            Some(7),
            "{name}: the open must fail with ENOSPC: {status:?} {stderr}"
        );
        assert_eq!(
            std::fs::read(&file).unwrap(),
            b"precious bytes",
            "{name}: a faulted truncate boundary must leave the file untouched"
        );
    }
}

/// `O_TRUNC` that does NOT mutate stays unseen: a read-only open, or a nonexistent path (which
/// fails with ENOENT, and with `O_CREAT` is already a `create`), is not a truncate boundary.
#[test]
fn o_trunc_without_a_mutation_is_not_a_truncate_boundary() {
    let dir = TempDir::new().expect("tempdir");
    let root = dir.path().canonicalize().unwrap();
    std::fs::write(root.join("keep.bin"), b"abc").expect("seed");
    let src = format!(
        r#"#define _GNU_SOURCE
#include <fcntl.h>
#include <unistd.h>
int main(void) {{
  int a = open("{r}/keep.bin", O_RDONLY | O_TRUNC); /* read-only: Linux ignores O_TRUNC? undefined; not a mutation request */
  if (a >= 0) close(a);
  int b = open("{r}/absent.bin", O_WRONLY | O_TRUNC); /* ENOENT */
  if (b >= 0) return 3;
  return 0;
}}"#,
        r = root.display()
    );
    let (status, log, stderr) = run_c("trunc_nomut", &src, &root, None);
    assert!(status.success(), "{status:?} {stderr}");
    assert!(
        boundary_number(&log, "truncate", "/absent.bin").is_none(),
        "ENOENT open is not a boundary: {log:?}"
    );
    assert!(
        boundary_number(&log, "truncate", "/keep.bin").is_none(),
        "a read-only open is not a writable truncate: {log:?}"
    );
}

/// `O_TMPFILE` is a create: counted as a `create` boundary on the directory, faultable, and its
/// mode argument is read (the file comes back with that mode).
#[test]
fn o_tmpfile_is_a_faultable_create_boundary_and_reads_its_mode() {
    let dir = TempDir::new().expect("tempdir");
    let root = dir.path().canonicalize().unwrap();
    let src = format!(
        r#"#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
int main(void) {{
  int fd = open("{r}", O_TMPFILE | O_WRONLY, 0640);
  if (fd < 0) return errno == ENOSPC ? 7 : (errno == EOPNOTSUPP || errno == EISDIR ? 9 : 3);
  struct stat st;
  if (fstat(fd, &st) != 0) return 4;
  close(fd);
  return (st.st_mode & 0777) == 0640 ? 0 : 5;
}}"#,
        r = root.display()
    );
    let (status, log, stderr) = run_c("tmpfile", &src, &root, None);
    if status.code() == Some(9) {
        panic!("FAIL CLOSED: this filesystem does not support O_TMPFILE under {root:?}; the probe cannot run ({stderr})");
    }
    assert_eq!(
        status.code(),
        Some(0),
        "mode argument must be honoured: {status:?} {stderr}"
    );
    let n = log
        .lines()
        .find_map(|l| {
            let mut it = l.splitn(3, ' ');
            let n: usize = it.next()?.parse().ok()?;
            (it.next()? == "create").then_some(n)
        })
        .unwrap_or_else(|| panic!("no `create` boundary for O_TMPFILE: {log:?}"));
    let (status, _, stderr) = run_c("tmpfile_fault", &src, &root, Some((n, "enospc")));
    assert_eq!(
        status.code(),
        Some(7),
        "faulted O_TMPFILE must fail with ENOSPC: {status:?} {stderr}"
    );
}
