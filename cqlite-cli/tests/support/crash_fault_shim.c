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
#include <sys/uio.h>
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
typedef long (*syscall_fn)(long, ...);
static syscall_fn real_syscall(void) {
  static syscall_fn f;
  if (!f) f = (syscall_fn)dlsym(RTLD_NEXT, "syscall");
  return f;
}
static void log_line(long n, const char *kind, const char *path) {
  if (!logpath[0]) return;
  syscall_fn sc = real_syscall();
  int fd = (int)sc(SYS_openat, AT_FDCWD, logpath, O_WRONLY | O_CREAT | O_APPEND, 0644);
  if (fd < 0) return;
  char buf[PATH_MAX + 64];
  int len = snprintf(buf, sizeof buf, "%ld %s %s\n", n, kind, path);
  sc(SYS_write, fd, buf, len);
  sc(SYS_close, fd);
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
  } else if (fd_path(dirfd, out, cap) != 0) {
    return -1; /* /proc/self/fd/<dirfd> resolution failed: path not attributable */
  }
  size_t l = strlen(out);
  snprintf(out + l, cap - l, "/%s", path);
  return 0;
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

int renameat2(int d1, const char *a, int d2, const char *b2, unsigned int flags) {
  REAL(renameat2);
  char p[PATH_MAX];
  if (abs_path(d2, b2, p, sizeof p) == 0) { int b = boundary("rename", p, 0); if (b > 0) { errno = b; return -1; } }
  return real(d1, a, d2, b2, flags);
}
int mkdirat(int dfd, const char *path, mode_t m) {
  REAL(mkdirat);
  char p[PATH_MAX];
  if (abs_path(dfd, path, p, sizeof p) == 0) { int b = boundary("mkdir", p, 0); if (b > 0) { errno = b; return -1; } }
  return real(dfd, path, m);
}
int rmdir(const char *path) {
  REAL(rmdir);
  char p[PATH_MAX];
  if (abs_path(AT_FDCWD, path, p, sizeof p) == 0) { int b = boundary("unlink", p, 0); if (b > 0) { errno = b; return -1; } }
  return real(path);
}
/* Vectored writes: a short fault persists the first half of the TOTAL byte count. */
static size_t iov_total(const struct iovec *iov, int n) {
  size_t t = 0;
  for (int i = 0; i < n; i++) t += iov[i].iov_len;
  return t;
}
static int iov_prefix(const struct iovec *iov, int n, size_t keep, struct iovec *out) {
  int k = 0;
  for (int i = 0; i < n && keep > 0; i++) {
    out[k].iov_base = iov[i].iov_base;
    out[k].iov_len = iov[i].iov_len < keep ? iov[i].iov_len : keep;
    keep -= out[k].iov_len;
    k++;
  }
  return k;
}
ssize_t writev(int fd, const struct iovec *iov, int n) {
  REAL(writev);
  char p[PATH_MAX];
  if (n > 0 && n <= 1024 && fd_path(fd, p, sizeof p) == 0) {
    int b = boundary("write", p, 1);
    if (b > 0) { errno = b; return -1; }
    if (b < 0) {
      size_t t = iov_total(iov, n);
      if (t < 2) { errno = ENOSPC; return -1; }
      struct iovec cut[1024];
      int k = iov_prefix(iov, n, t / 2, cut);
      return real(fd, cut, k);
    }
  }
  return real(fd, iov, n);
}
ssize_t pwritev(int fd, const struct iovec *iov, int n, off_t off) {
  REAL(pwritev);
  char p[PATH_MAX];
  if (n > 0 && n <= 1024 && fd_path(fd, p, sizeof p) == 0) {
    int b = boundary("pwrite", p, 1);
    if (b > 0) { errno = b; return -1; }
    if (b < 0) {
      size_t t = iov_total(iov, n);
      if (t < 2) { errno = ENOSPC; return -1; }
      struct iovec cut[1024];
      int k = iov_prefix(iov, n, t / 2, cut);
      return real(fd, cut, k, off);
    }
  }
  return real(fd, iov, n, off);
}
ssize_t pwritev64(int fd, const struct iovec *iov, int n, off64_t off) {
  REAL(pwritev64);
  char p[PATH_MAX];
  if (n > 0 && n <= 1024 && fd_path(fd, p, sizeof p) == 0) {
    int b = boundary("pwrite", p, 1);
    if (b > 0) { errno = b; return -1; }
    if (b < 0) {
      size_t t = iov_total(iov, n);
      if (t < 2) { errno = ENOSPC; return -1; }
      struct iovec cut[1024];
      int k = iov_prefix(iov, n, t / 2, cut);
      return real(fd, cut, k, off);
    }
  }
  return real(fd, iov, n, off);
}
ssize_t pwritev2(int fd, const struct iovec *iov, int n, off_t off, int flags) {
  REAL(pwritev2);
  char p[PATH_MAX];
  if (n > 0 && n <= 1024 && fd_path(fd, p, sizeof p) == 0) {
    int b = boundary("pwrite", p, 1);
    if (b > 0) { errno = b; return -1; }
    if (b < 0) {
      size_t t = iov_total(iov, n);
      if (t < 2) { errno = ENOSPC; return -1; }
      struct iovec cut[1024];
      int k = iov_prefix(iov, n, t / 2, cut);
      return real(fd, cut, k, off, flags);
    }
  }
  return real(fd, iov, n, off, flags);
}
ssize_t splice(int fi, off64_t *oi, int fo, off64_t *oo, size_t len, unsigned int fl) {
  REAL(splice);
  char p[PATH_MAX];
  if (fd_path(fo, p, sizeof p) == 0) {
    int b = boundary("copy", p, 1);
    if (b > 0) { errno = b; return -1; }
    if (b < 0) { if (len < 2) { errno = ENOSPC; return -1; } return real(fi, oi, fo, oo, len / 2, fl); }
  }
  return real(fi, oi, fo, oo, len, fl);
}
int fallocate(int fd, int mode, off_t off, off_t len) {
  REAL(fallocate);
  char p[PATH_MAX];
  if (fd_path(fd, p, sizeof p) == 0) { int b = boundary("truncate", p, 0); if (b > 0) { errno = b; return -1; } }
  return real(fd, mode, off, len);
}
int posix_fallocate(int fd, off_t off, off_t len) {
  REAL(posix_fallocate);
  char p[PATH_MAX];
  if (fd_path(fd, p, sizeof p) == 0) { int b = boundary("truncate", p, 0); if (b > 0) return b; }
  return real(fd, off, len);
}
int link(const char *a, const char *b2) {
  REAL(link);
  char p[PATH_MAX];
  if (abs_path(AT_FDCWD, b2, p, sizeof p) == 0) { int b = boundary("create", p, 0); if (b > 0) { errno = b; return -1; } }
  return real(a, b2);
}
int symlink(const char *a, const char *b2) {
  REAL(symlink);
  char p[PATH_MAX];
  if (abs_path(AT_FDCWD, b2, p, sizeof p) == 0) { int b = boundary("create", p, 0); if (b > 0) { errno = b; return -1; } }
  return real(a, b2);
}

/* `syscall(2)` itself: std and third-party crates call it directly for some operations,
 * bypassing every libc wrapper above. The mutating numbers are decoded with their RAW kernel ABI
 * (x86_64 / aarch64 argument order, which differs from the libc wrappers for the *v2 calls) and
 * routed through the wrappers so they are counted and faulted identically.
 *
 * Defined with a FIXED arity under the symbol name `syscall` (`__asm__` label) instead of as a C
 * variadic: a variadic definition that va_arg's six longs reads arguments the caller never
 * supplied (undefined behaviour), whereas on the SysV x86_64/aarch64 ABIs a fixed seven-register
 * signature merely reads registers the caller left unspecified and then ignores the unused ones.
 * That is also what makes forwarding an unhandled number to the real `syscall` register-exact.
 *
 * Every raw number that is neither handled above nor on the non-mutating ALLOWLIST below ABORTS
 * the run loudly (exit 97, number named): a harness that cannot see a write boundary must fail,
 * never go green blind. */
long shim_syscall(long num, long a0, long a1, long a2, long a3, long a4, long a5) __asm__("syscall");
static void blind_abort(long num) {
  static const char msg[] = "crash_fault_shim: UNHANDLED raw syscall number ";
  char buf[sizeof msg + 24];
  int len = snprintf(buf, sizeof buf, "%s%ld\n", msg, num);
  real_syscall()(SYS_write, 2, buf, len);
  real_syscall()(SYS_exit_group, 97);
}
long shim_syscall(long num, long a0, long a1, long a2, long a3, long a4, long a5) {
  long r;
  switch (num) {
#define RET(call) do { r = (long)(call); return r; } while (0)
    case SYS_write: RET(write((int)a0, (const void *)a1, (size_t)a2));
    case SYS_pwrite64: RET(pwrite64((int)a0, (const void *)a1, (size_t)a2, (off64_t)a3));
    case SYS_writev: RET(writev((int)a0, (const struct iovec *)a1, (int)a2));
    /* raw pwritev(fd, iov, cnt, pos_l, pos_h); pos_h is 0 on 64-bit. */
    case SYS_pwritev: RET(pwritev((int)a0, (const struct iovec *)a1, (int)a2, (off_t)a3));
    /* raw pwritev2(fd, iov, cnt, pos_l, pos_h, flags): flags is the SIXTH argument. */
    case SYS_pwritev2: RET(pwritev2((int)a0, (const struct iovec *)a1, (int)a2, (off_t)a3, (int)a5));
    case SYS_copy_file_range: RET(copy_file_range((int)a0, (off_t *)a1, (int)a2, (off_t *)a3, (size_t)a4, (unsigned)a5));
    case SYS_sendfile: RET(sendfile((int)a0, (int)a1, (off_t *)a2, (size_t)a3));
    case SYS_splice: RET(splice((int)a0, (off64_t *)a1, (int)a2, (off64_t *)a3, (size_t)a4, (unsigned)a5));
    case SYS_fsync: RET(fsync((int)a0));
    case SYS_fdatasync: RET(fdatasync((int)a0));
    case SYS_ftruncate: RET(ftruncate((int)a0, (off_t)a1));
    case SYS_fallocate: RET(fallocate((int)a0, (int)a1, (off_t)a2, (off_t)a3));
    case SYS_renameat: RET(renameat((int)a0, (const char *)a1, (int)a2, (const char *)a3));
    case SYS_renameat2: RET(renameat2((int)a0, (const char *)a1, (int)a2, (const char *)a3, (unsigned)a4));
    case SYS_unlinkat: RET(unlinkat((int)a0, (const char *)a1, (int)a2));
    case SYS_mkdirat: RET(mkdirat((int)a0, (const char *)a1, (mode_t)a2));
    case SYS_openat: RET(openat((int)a0, (const char *)a1, (int)a2, (mode_t)a3));
#ifdef SYS_rename
    case SYS_rename: RET(rename((const char *)a0, (const char *)a1));
#endif
#ifdef SYS_unlink
    case SYS_unlink: RET(unlink((const char *)a0));
#endif
#ifdef SYS_rmdir
    case SYS_rmdir: RET(rmdir((const char *)a0));
#endif
#ifdef SYS_mkdir
    case SYS_mkdir: RET(mkdir((const char *)a0, (mode_t)a1));
#endif
#ifdef SYS_open
    case SYS_open: RET(open((const char *)a0, (int)a1, (mode_t)a2));
#endif
#ifdef SYS_creat
    case SYS_creat: RET(creat((const char *)a0, (mode_t)a1));
#endif
#undef RET
    /* ALLOWLIST of raw syscalls that cannot mutate the filesystem: forwarded uncounted. This is
     * deliberately NOT a denylist - a mutating call nobody listed (mknodat, fchmodat, fchownat,
     * the xattr family, ...) must never pass unseen. MEASURED: a counting run of rebuild and
     * salvage over every harness fixture issues exactly one raw number through `syscall()` -
     * futex (the rest of the process's I/O goes through the libc wrappers interposed above).
     * The remaining entries are the usual read-only / time / memory / scheduling numbers a
     * runtime may use. Anything else (incl. ioctl, which can clone/dedupe file data) aborts. */
#define ALLOW(nr) case nr:
#ifdef SYS_futex
    ALLOW(SYS_futex)
#endif
#ifdef SYS_futex_waitv
    ALLOW(SYS_futex_waitv)
#endif
#ifdef SYS_getrandom
    ALLOW(SYS_getrandom)
#endif
#ifdef SYS_gettid
    ALLOW(SYS_gettid)
#endif
#ifdef SYS_getpid
    ALLOW(SYS_getpid)
#endif
#ifdef SYS_getcpu
    ALLOW(SYS_getcpu)
#endif
#ifdef SYS_rseq
    ALLOW(SYS_rseq)
#endif
#ifdef SYS_membarrier
    ALLOW(SYS_membarrier)
#endif
#ifdef SYS_sched_yield
    ALLOW(SYS_sched_yield)
#endif
#ifdef SYS_sched_getaffinity
    ALLOW(SYS_sched_getaffinity)
#endif
#ifdef SYS_clock_gettime
    ALLOW(SYS_clock_gettime)
#endif
#ifdef SYS_clock_nanosleep
    ALLOW(SYS_clock_nanosleep)
#endif
#ifdef SYS_nanosleep
    ALLOW(SYS_nanosleep)
#endif
#ifdef SYS_read
    ALLOW(SYS_read)
#endif
#ifdef SYS_pread64
    ALLOW(SYS_pread64)
#endif
#ifdef SYS_lseek
    ALLOW(SYS_lseek)
#endif
#ifdef SYS_close
    ALLOW(SYS_close)
#endif
#ifdef SYS_getdents64
    ALLOW(SYS_getdents64)
#endif
#ifdef SYS_statx
    ALLOW(SYS_statx)
#endif
#ifdef SYS_newfstatat
    ALLOW(SYS_newfstatat)
#endif
#ifdef SYS_fstat
    ALLOW(SYS_fstat)
#endif
#ifdef SYS_readlinkat
    ALLOW(SYS_readlinkat)
#endif
#ifdef SYS_faccessat
    ALLOW(SYS_faccessat)
#endif
#ifdef SYS_getcwd
    ALLOW(SYS_getcwd)
#endif
#ifdef SYS_mmap
    ALLOW(SYS_mmap)
#endif
#ifdef SYS_munmap
    ALLOW(SYS_munmap)
#endif
#ifdef SYS_mprotect
    ALLOW(SYS_mprotect)
#endif
#ifdef SYS_madvise
    ALLOW(SYS_madvise)
#endif
#ifdef SYS_epoll_ctl
    ALLOW(SYS_epoll_ctl)
#endif
#ifdef SYS_epoll_pwait
    ALLOW(SYS_epoll_pwait)
#endif
#ifdef SYS_eventfd2
    ALLOW(SYS_eventfd2)
#endif
#ifdef SYS_prlimit64
    ALLOW(SYS_prlimit64)
#endif
#ifdef SYS_uname
    ALLOW(SYS_uname)
#endif
#undef ALLOW
    {
      /* AUDIT hook: record every allow-listed raw number the process issues (the measurement). */
      const char *audit = getenv("CQLITE_FAULT_SYSCALL_AUDIT");
      if (audit) {
        char buf[32];
        int len = snprintf(buf, sizeof buf, "%ld\n", num);
        int fd = (int)real_syscall()(SYS_openat, AT_FDCWD, audit, O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd >= 0) { real_syscall()(SYS_write, fd, buf, len); real_syscall()(SYS_close, fd); }
      }
      return real_syscall()(num, a0, a1, a2, a3, a4, a5);
    }
    default:
      blind_abort(num);
      return -1;
  }
}
