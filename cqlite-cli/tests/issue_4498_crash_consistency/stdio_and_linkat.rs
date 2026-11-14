//! Shim coverage probes (#4498): glibc flushes a stdio `FILE` through its INTERNAL write, never
//! the interposed `write(2)`, so `fwrite`/`fputc` are interposed in their own right; `linkat`
//! creates a directory entry and is a faultable `create` boundary like `link`.
use super::{run_c_under_shim, shim};
use std::process::Command;
use tempfile::TempDir;

const STDIO: &str = r#"#include <stdio.h>
int main(int c, char **v) {
  FILE *f = fopen(v[1], "w");
  if (!f) return 4;
  size_t n = fwrite("hello", 1, 5, f);
  int k = fputc('x', f);
  fflush(f); fclose(f);
  return (n == 5 && k == 'x') ? 0 : 3;
}"#;

fn root() -> (TempDir, std::path::PathBuf) {
    let d = TempDir::new().expect("tempdir");
    let r = d.path().canonicalize().unwrap();
    (d, r)
}

#[test]
fn fwrite_and_fputc_are_counted_as_write_boundaries() {
    let (_d, root) = root();
    let src = STDIO.replace("v[1]", &format!("\"{}\"", root.join("f").display()));
    let (status, log, _) = run_c_under_shim("stdio", &src, &root);
    assert!(status.success(), "stdio program failed: {status:?}");
    let n = log
        .lines()
        .filter(|l| l.contains(" write ") && l.ends_with("/f"))
        .count();
    assert_eq!(
        n, 2,
        "fwrite + fputc must each log one write boundary: {log:?}"
    );
    assert_eq!(std::fs::read(root.join("f")).unwrap(), b"hellox");
}

#[test]
fn fwrite_boundary_can_be_faulted() {
    let (_d, root) = root();
    let tmp = TempDir::new().unwrap();
    let (c, exe) = (tmp.path().join("s.c"), tmp.path().join("s"));
    std::fs::write(&c, STDIO).unwrap();
    let cc = Command::new("cc")
        .arg(&c)
        .arg("-o")
        .arg(&exe)
        .output()
        .expect("cc");
    assert!(
        cc.status.success(),
        "{}",
        String::from_utf8_lossy(&cc.stderr)
    );
    let run = Command::new(&exe)
        .arg(root.join("f"))
        .env("LD_PRELOAD", shim())
        .env("CQLITE_FAULT_ROOT", &root)
        .env("CQLITE_FAULT_AT", "1")
        .env("CQLITE_FAULT_MODE", "enospc")
        .output()
        .expect("run");
    assert_eq!(
        run.status.code(),
        Some(3),
        "the faulted fwrite must surface as a failure"
    );
    // Only boundary 1 (the fwrite) is faulted: "hello" never lands, the later fputc does.
    assert_eq!(std::fs::read(root.join("f")).unwrap(), b"x");
}

#[test]
fn linkat_is_a_counted_create_boundary() {
    let (_d, root) = root();
    std::fs::write(root.join("a"), b"x").unwrap();
    let src = format!(
        "#define _GNU_SOURCE\n#include <fcntl.h>\n#include <unistd.h>\nint main(void) {{ return linkat(AT_FDCWD, \"{0}/a\", AT_FDCWD, \"{0}/b\", 0); }}",
        root.display()
    );
    let (status, log, _) = run_c_under_shim("linkat", &src, &root);
    assert!(status.success(), "linkat failed: {status:?}");
    assert!(
        log.lines()
            .any(|l| l.contains(" create ") && l.ends_with("/b")),
        "{log:?}"
    );
    assert!(root.join("b").exists());
}
