/* Issue #4498 — crash-consistency fault-injection shim (TEST-ONLY, LD_PRELOAD).
 *
 * Why an LD_PRELOAD shim instead of hooks in the product: the verbs under test
 * (`salvage`, `rebuild`) write through a dozen independent writers (std::fs, BufWriter,
 * fs::copy, sync_all, ...). Interposing the libc calls they all bottom out in enumerates
 * EVERY write / fsync / rename / create / unlink boundary of the real, shipped CLI binary
 * without adding a single line of fault code to it — so there is nothing to compile out
 * of release builds, and the AC7 proof is "the shipped binary contains no fault hook".
 * A crash is a real SIGKILL (no destructor, no cleanup path runs), which an in-process
 * error injection cannot model.
 *
 * Protocol (all via environment):
 *   CQLITE_FAULT_ROOT  absolute directory; only boundaries under it are counted.
 *   CQLITE_FAULT_LOG   file to append one line per counted boundary: "<n> <kind> <path>".
 *   CQLITE_FAULT_AT    1-based boundary number to fault (absent/0 = counting run only).
 *   CQLITE_FAULT_MODE  crash   - SIGKILL before the boundary takes effect
 *                      enospc  - the boundary fails with ENOSPC, effect not applied
 *                      short   - a write persists HALF its bytes and returns the short count,
 *                                then every later write-class boundary fails ENOSPC (a full
 *                                disk; a bare short write would just be retried by write_all)
 *                                [non-write boundaries behave as enospc]
 */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <signal.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/sendfile.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <unistd.h>

enum { M_COUNT = 0, M_CRASH = 1, M_ENOSPC = 2, M_SHORT = 3 };
static char root[PATH_MAX];
static size_t root_len;
static char logpath[PATH_MAX];
static long fault_at;
static int mode;
static atomic_long counter;
static atomic_int disk_full;

__attribute__((constructor)) static void init(void) {
  const char *r = getenv("CQLITE_FAULT_ROOT");
  if (r) { strncpy(root, r, sizeof root - 1); root_len = strlen(root); }
  const char *l = getenv("CQLITE_FAULT_LOG");
  if (l) strncpy(logpath, l, sizeof logpath - 1);
  const char *a = getenv("CQLITE_FAULT_AT");
  fault_at = a ? atol(a) : 0;
  const char *m = getenv("CQLITE_FAULT_MODE");
  if (m) mode = !strcmp(m, "crash") ? M_CRASH : !strcmp(m, "enospc") ? M_ENOSPC : !strcmp(m, "short") ? M_SHORT : M_COUNT;
}

static int under_root(const char *p) {
  return root_len && p && strncmp(p, root, root_len) == 0 && (p[root_len] == '/' || p[root_len] == 0);
}
static int fd_path(int fd, char *out, size_t cap) {
  char link[64];
  snprintf(link, sizeof link, "/proc/self/fd/%d", fd);
  ssize_t n = readlink(link, out, cap - 1);
  if (n < 0) return -1;
  out[n] = 0;
  return 0;
}
static void log_line(long n, const char *kind, const char *path) {
  if (!logpath[0]) return;
  int fd = (int)syscall(SYS_openat, AT_FDCWD, logpath, O_WRONLY | O_CREAT | O_APPEND, 0644);
  if (fd < 0) return;
  char buf[PATH_MAX + 64];
  int len = snprintf(buf, sizeof buf, "%ld %s %s\n", n, kind, path);
  syscall(SYS_write, fd, buf, len);
  syscall(SYS_close, fd);
}

/* 0 = proceed; >0 = fail with that errno; -1 = apply a SHORT write (caller halves it). */
static int boundary(const char *kind, const char *path, int is_write) {
  if (!under_root(path)) return 0;
  long n = atomic_fetch_add(&counter, 1) + 1;
  log_line(n, kind, path);
  if (atomic_load(&disk_full) && is_write) return ENOSPC;
  if (fault_at && n == fault_at) {
    if (mode == M_CRASH) { kill(getpid(), SIGKILL); for (;;) pause(); }
    if (mode == M_ENOSPC) return ENOSPC;
    if (mode == M_SHORT) {
      atomic_store(&disk_full, 1);
      return is_write ? -1 : ENOSPC;
    }
  }
  return 0;
}

#define REAL(name) static __typeof__(&name) real = 0; if (!real) real = (__typeof__(&name))dlsym(RTLD_NEXT, #name)

ssize_t write(int fd, const void *buf, size_t n) {
  REAL(write);
  char p[PATH_MAX];
  if (fd_path(fd, p, sizeof p) == 0) {
    int b = boundary("write", p, 1);
    if (b > 0) { errno = b; return -1; }
    if (b < 0) { if (n < 2) { errno = ENOSPC; return -1; } return real(fd, buf, n / 2); }
  }
  return real(fd, buf, n);
}
ssize_t pwrite(int fd, const void *buf, size_t n, off_t off) {
  REAL(pwrite);
  char p[PATH_MAX];
  if (fd_path(fd, p, sizeof p) == 0) {
    int b = boundary("pwrite", p, 1);
    if (b > 0) { errno = b; return -1; }
    if (b < 0) { if (n < 2) { errno = ENOSPC; return -1; } return real(fd, buf, n / 2, off); }
  }
  return real(fd, buf, n, off);
}
ssize_t pwrite64(int fd, const void *buf, size_t n, off64_t off) {
  REAL(pwrite64);
  char p[PATH_MAX];
  if (fd_path(fd, p, sizeof p) == 0) {
    int b = boundary("pwrite", p, 1);
    if (b > 0) { errno = b; return -1; }
    if (b < 0) { if (n < 2) { errno = ENOSPC; return -1; } return real(fd, buf, n / 2, off); }
  }
  return real(fd, buf, n, off);
}
ssize_t copy_file_range(int fi, off_t *oi, int fo, off_t *oo, size_t len, unsigned int fl) {
  REAL(copy_file_range);
  char p[PATH_MAX];
  if (fd_path(fo, p, sizeof p) == 0) {
    int b = boundary("copy", p, 1);
    if (b > 0) { errno = b; return -1; }
    if (b < 0) { if (len < 2) { errno = ENOSPC; return -1; } return real(fi, oi, fo, oo, len / 2, fl); }
  }
  return real(fi, oi, fo, oo, len, fl);
}
ssize_t sendfile(int fo, int fi, off_t *off, size_t len) {
  REAL(sendfile);
  char p[PATH_MAX];
  if (fd_path(fo, p, sizeof p) == 0) {
    int b = boundary("copy", p, 1);
    if (b > 0) { errno = b; return -1; }
    if (b < 0) { if (len < 2) { errno = ENOSPC; return -1; } return real(fo, fi, off, len / 2); }
  }
  return real(fo, fi, off, len);
}
ssize_t sendfile64(int fo, int fi, off64_t *off, size_t len) {
  REAL(sendfile64);
  char p[PATH_MAX];
  if (fd_path(fo, p, sizeof p) == 0) {
    int b = boundary("copy", p, 1);
    if (b > 0) { errno = b; return -1; }
    if (b < 0) { if (len < 2) { errno = ENOSPC; return -1; } return real(fo, fi, off, len / 2); }
  }
  return real(fo, fi, off, len);
}
int fsync(int fd) {
  REAL(fsync);
  char p[PATH_MAX];
  if (fd_path(fd, p, sizeof p) == 0) { int b = boundary("fsync", p, 0); if (b > 0) { errno = b; return -1; } }
  return real(fd);
}
int fdatasync(int fd) {
  REAL(fdatasync);
  char p[PATH_MAX];
  if (fd_path(fd, p, sizeof p) == 0) { int b = boundary("fsync", p, 0); if (b > 0) { errno = b; return -1; } }
  return real(fd);
}
int ftruncate(int fd, off_t len) {
  REAL(ftruncate);
  char p[PATH_MAX];
  if (fd_path(fd, p, sizeof p) == 0) { int b = boundary("truncate", p, 0); if (b > 0) { errno = b; return -1; } }
  return real(fd, len);
}
int ftruncate64(int fd, off64_t len) {
  REAL(ftruncate64);
  char p[PATH_MAX];
  if (fd_path(fd, p, sizeof p) == 0) { int b = boundary("truncate", p, 0); if (b > 0) { errno = b; return -1; } }
  return real(fd, len);
}
/* Absolute path of (dirfd, path) when it is absolute or AT_FDCWD-relative. */
static int abs_path(int dirfd, const char *path, char *out, size_t cap) {
  if (path[0] == '/') { strncpy(out, path, cap - 1); out[cap - 1] = 0; return 0; }
  if (dirfd == AT_FDCWD) {
    if (!getcwd(out, cap)) return -1;
    size_t l = strlen(out);
    snprintf(out + l, cap - l, "/%s", path);
    return 0;
  }
  return -1;
}
static int create_boundary(int dirfd, const char *path, int flags) {
  char p[PATH_MAX];
  if ((flags & O_CREAT) && abs_path(dirfd, path, p, sizeof p) == 0) return boundary("create", p, 0);
  return 0;
}
int open(const char *path, int flags, ...) {
  REAL(open);
  mode_t m = 0; if (flags & O_CREAT) { va_list ap; va_start(ap, flags); m = va_arg(ap, mode_t); va_end(ap); }
  int b = create_boundary(AT_FDCWD, path, flags); if (b > 0) { errno = b; return -1; }
  return real(path, flags, m);
}
int open64(const char *path, int flags, ...) {
  REAL(open64);
  mode_t m = 0; if (flags & O_CREAT) { va_list ap; va_start(ap, flags); m = va_arg(ap, mode_t); va_end(ap); }
  int b = create_boundary(AT_FDCWD, path, flags); if (b > 0) { errno = b; return -1; }
  return real(path, flags, m);
}
int openat(int dfd, const char *path, int flags, ...) {
  REAL(openat);
  mode_t m = 0; if (flags & O_CREAT) { va_list ap; va_start(ap, flags); m = va_arg(ap, mode_t); va_end(ap); }
  int b = create_boundary(dfd, path, flags); if (b > 0) { errno = b; return -1; }
  return real(dfd, path, flags, m);
}
int openat64(int dfd, const char *path, int flags, ...) {
  REAL(openat64);
  mode_t m = 0; if (flags & O_CREAT) { va_list ap; va_start(ap, flags); m = va_arg(ap, mode_t); va_end(ap); }
  int b = create_boundary(dfd, path, flags); if (b > 0) { errno = b; return -1; }
  return real(dfd, path, flags, m);
}
int creat(const char *path, mode_t m) { return open(path, O_CREAT | O_WRONLY | O_TRUNC, m); }
int creat64(const char *path, mode_t m) { return open64(path, O_CREAT | O_WRONLY | O_TRUNC, m); }
int rename(const char *a, const char *b2) {
  REAL(rename);
  char p[PATH_MAX];
  if (abs_path(AT_FDCWD, b2, p, sizeof p) == 0) { int b = boundary("rename", p, 0); if (b > 0) { errno = b; return -1; } }
  return real(a, b2);
}
int renameat(int d1, const char *a, int d2, const char *b2) {
  REAL(renameat);
  char p[PATH_MAX];
  if (abs_path(d2, b2, p, sizeof p) == 0) { int b = boundary("rename", p, 0); if (b > 0) { errno = b; return -1; } }
  return real(d1, a, d2, b2);
}
int unlink(const char *path) {
  REAL(unlink);
  char p[PATH_MAX];
  if (abs_path(AT_FDCWD, path, p, sizeof p) == 0) { int b = boundary("unlink", p, 0); if (b > 0) { errno = b; return -1; } }
  return real(path);
}
int unlinkat(int dfd, const char *path, int flags) {
  REAL(unlinkat);
  char p[PATH_MAX];
  if (abs_path(dfd, path, p, sizeof p) == 0) { int b = boundary("unlink", p, 0); if (b > 0) { errno = b; return -1; } }
  return real(dfd, path, flags);
}
int mkdir(const char *path, mode_t m) {
  REAL(mkdir);
  char p[PATH_MAX];
  if (abs_path(AT_FDCWD, path, p, sizeof p) == 0) { int b = boundary("mkdir", p, 0); if (b > 0) { errno = b; return -1; } }
  return real(path, m);
}
