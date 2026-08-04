/*
 * cache-hostile.c — WS0 #3224 positive-control microbenchmark.
 *
 * Purpose: give `positive-control.sh` a workload whose memory behaviour is known
 * BEFORE it is measured, so that a PMU counter can be checked against a prediction
 * instead of against a hope. #3217 was lost to the silent-instrument class: on its
 * virtualized host `cache-references` did NOT report `<not supported>` — it
 * programmed cleanly and returned a hard 0. A smoke test ("is the counter
 * non-zero?") does not catch a counter stuck at a constant, and a run that trusts
 * such a counter produces a confident, wrong report.
 *
 * DESIGN — one differential, two arms, IDENTICAL code path:
 *
 *   arm "friendly": pointer-chase confined to the FIRST --working-kib of the
 *                   allocation. Sized to fit in L2. Every load hits L1/L2 and
 *                   therefore never reaches the LLC.
 *   arm "hostile":  pointer-chase over the WHOLE allocation, sized many times the
 *                   LLC. A random single-cycle permutation over 64 B nodes, chased
 *                   through a serial data dependency: the prefetcher cannot help
 *                   (next address is not known until the current load retires) and
 *                   the working set cannot be cached. Every load is an LLC miss.
 *
 * Both arms are the SAME allocation, the SAME loop, the SAME instruction count and
 * the SAME total access count. The only variable is the extent of the working set.
 * That is what makes the comparison a control: an instruction-count ratio near 1.0
 * with a large cycles ratio proves the difference is purely memory-hierarchy, and
 * any counter that claims to see the memory hierarchy MUST move with it.
 *
 * ONLY THE CHASE MAY BE COUNTED — this is measured, not assumed. Two phases outside
 * the chase are large and, worse, ASYMMETRIC between the arms:
 *   - init (mmap + page faults + permutation build): huge in the hostile arm, ~free
 *     in the friendly arm, which only ever touches --working-kib;
 *   - exit-time address-space teardown: measured on a 512 MiB buffer at 192M
 *     instructions (hostile, all pages resident) vs 80M (friendly, few resident) —
 *     i.e. teardown alone can exceed the chase and it does NOT cancel between arms.
 * Counting either one corrupts the differential in an unpredictable direction, so
 * the measured window is gated EXACTLY around the chase loop using perf's control
 * FIFO (`perf stat -D -1 --control fifo:<ctl>,<ack>`): the program writes `enable`
 * immediately before the loop and `disable` immediately after, and every handshake
 * has a deadline so a missing peer fails loudly instead of hanging.
 *
 * `--delay-ms` remains as a standalone fallback for running this benchmark without
 * the control FIFO; in that mode init is excluded by `perf stat -D <ms>` and the
 * program exits 4 (`init_overrun=1`) if init overran the deadline — but teardown is
 * then INSIDE the window, so that mode is for ad-hoc use, not for the gate.
 *
 * Also provides `stream` mode: a STREAM-triad-class bandwidth reference measured on
 * the same host, for AC5 (measured bandwidth vs achievable peak). It is NOT the
 * vendor STREAM benchmark and is labelled as such everywhere it is reported.
 *
 * Dependency-light on purpose: C99 + pthreads, built with plain `cc`. It must run on
 * a fresh bare-metal box before any CQLite build exists.
 *
 * Build:  cc -O2 -std=c99 -pthread -o cache-hostile cache-hostile.c
 */
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <poll.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <time.h>
#include <unistd.h>

#define LINE_BYTES 64u                    /* one cache line per chase node */
#define WORDS_PER_LINE (LINE_BYTES / 8u)  /* stride, in uint64_t */

static double now_s(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec * 1e-9;
}

static double g_start;

/* splitmix64 — deterministic, seeded, so a rerun chases the same permutation. */
static uint64_t rnd(uint64_t *s) {
    uint64_t z = (*s += 0x9E3779B97F4A7C15ULL);
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
    return z ^ (z >> 31);
}

static void die(const char *msg) {
    fprintf(stderr, "cache-hostile: FATAL: %s\n", msg);
    exit(2);
}

static void *alloc_buf(size_t bytes) {
    void *p = mmap(NULL, bytes, PROT_READ | PROT_WRITE,
                   MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (p == MAP_FAILED) die("mmap failed (buffer too large for this host?)");
    return p;
}

/*
 * Wait out the perf `-D <delay_ms>` window. Counting starts at `delay_s` after
 * process start; anything before it (mmap, page faults, permutation build) is
 * deliberately outside the measured window.
 */
static void wait_for_window(double delay_s, double init_s) {
    double elapsed = now_s() - g_start;
    if (elapsed >= delay_s) {
        printf("init_overrun=1\ninit_s=%.3f\ndelay_s=%.3f\n", init_s, delay_s);
        fprintf(stderr,
                "cache-hostile: INIT OVERRAN THE MEASUREMENT DELAY "
                "(init %.3fs >= delay %.3fs). The perf window would have counted "
                "buffer initialisation, which is common to both arms and would bias "
                "the ratio toward 1.0. Re-run with a larger --delay-ms.\n",
                init_s, delay_s);
        exit(4);
    }
    struct timespec ts;
    double rem = delay_s - elapsed;
    ts.tv_sec = (time_t)rem;
    ts.tv_nsec = (long)((rem - (double)ts.tv_sec) * 1e9);
    nanosleep(&ts, NULL);
}

/* ------------------------------------------------- perf control-FIFO handshake */

static int g_ctl_fd = -1, g_ack_fd = -1;

/*
 * perf opens the ctl FIFO for reading and the ack FIFO for writing before it execs
 * this workload, so O_WRONLY on ctl succeeds as soon as perf is ready. Every wait
 * carries a deadline: a hung handshake on a metered bare-metal box is worse than a
 * loud failure, and a silent one would leave the window ungated.
 */
static void ctl_open(const char *ctl, const char *ack) {
    double deadline = now_s() + 30.0;
    for (;;) {
        g_ctl_fd = open(ctl, O_WRONLY | O_NONBLOCK);
        if (g_ctl_fd >= 0) break;
        if (errno != ENXIO && errno != ENOENT) die("cannot open --ctl-fifo");
        if (now_s() > deadline) die("timed out waiting for perf to open --ctl-fifo");
        usleep(20000);
    }
    if (fcntl(g_ctl_fd, F_SETFL, 0) < 0) die("fcntl on ctl fifo failed");
    g_ack_fd = open(ack, O_RDONLY | O_NONBLOCK);
    if (g_ack_fd < 0) die("cannot open --ack-fifo");
}

static void ctl_cmd(const char *cmd) {
    char buf[32];
    if (write(g_ctl_fd, cmd, strlen(cmd)) < 0) die("write to perf ctl fifo failed");
    struct pollfd p = { .fd = g_ack_fd, .events = POLLIN };
    int r = poll(&p, 1, 30000);
    if (r <= 0) die("perf did not acknowledge a control command within 30s");
    if (read(g_ack_fd, buf, sizeof buf) < 0) die("read from perf ack fifo failed");
}

/* ------------------------------------------------------------------ chase mode */

static int run_chase(size_t buf_mib, size_t working_kib, uint64_t accesses,
                     double delay_s, uint64_t seed, const char *arm,
                     const char *ctl, const char *ack) {
    size_t buf_bytes = buf_mib << 20;
    size_t work_bytes = (working_kib == 0) ? buf_bytes : (working_kib << 10);
    if (work_bytes > buf_bytes) die("--working-kib exceeds --buffer-mib");
    uint64_t nodes = (uint64_t)(work_bytes / LINE_BYTES);
    if (nodes < 2) die("working set too small (need >= 2 cache lines)");

    double t0 = now_s();
    uint64_t *buf = (uint64_t *)alloc_buf(buf_bytes);

    /* Identity, then Sattolo's algorithm: yields a permutation that is a SINGLE
     * n-cycle, so the chase visits every node exactly once per lap. Random order
     * over cache lines is what defeats the hardware prefetcher. */
    for (uint64_t i = 0; i < nodes; i++) buf[i * WORDS_PER_LINE] = i;
    uint64_t rs = seed ? seed : 1;
    for (uint64_t i = nodes - 1; i > 0; i--) {
        uint64_t j = rnd(&rs) % i; /* strictly j < i => single cycle */
        uint64_t t = buf[i * WORDS_PER_LINE];
        buf[i * WORDS_PER_LINE] = buf[j * WORDS_PER_LINE];
        buf[j * WORDS_PER_LINE] = t;
    }
    double init_s = now_s() - t0;

    const char *gate = "delay";
    if (ctl && ack) { gate = "fifo"; ctl_open(ctl, ack); ctl_cmd("enable\n"); }
    else wait_for_window(delay_s, init_s);

    /* THE MEASURED SECTION. Serial dependency: each load's address comes from the
     * previous load's result, so there is no memory-level parallelism to hide a
     * miss behind, and the loop body is identical in both arms. */
    double c0 = now_s();
    uint64_t idx = 0, sum = 0;
    for (uint64_t k = 0; k < accesses; k++) {
        idx = buf[idx * WORDS_PER_LINE];
        sum += idx;
    }
    double chase_s = now_s() - c0;
    if (ctl && ack) ctl_cmd("disable\n");   /* teardown must land OUTSIDE the window */

    printf("mode=chase\narm=%s\ngate=%s\nbuffer_bytes=%zu\nworking_set_bytes=%zu\n",
           arm, gate, buf_bytes, work_bytes);
    printf("nodes=%" PRIu64 "\naccesses=%" PRIu64 "\ninit_s=%.4f\nchase_s=%.4f\n",
           nodes, accesses, init_s, chase_s);
    printf("ns_per_access=%.3f\nchecksum=%" PRIu64 "\ninit_overrun=0\n",
           chase_s * 1e9 / (double)accesses, sum);
    munmap(buf, buf_bytes);
    return 0;
}

/* ----------------------------------------------------------------- stream mode */

typedef struct {
    double *a, *b, *c;
    size_t lo, hi;
    pthread_barrier_t *bar;
    int iters;
} tri_arg_t;

static void *tri_worker(void *v) {
    tri_arg_t *t = (tri_arg_t *)v;
    const double scalar = 3.0;
    for (int it = 0; it < t->iters; it++) {
        pthread_barrier_wait(t->bar);
        for (size_t i = t->lo; i < t->hi; i++) t->a[i] = t->b[i] + scalar * t->c[i];
        pthread_barrier_wait(t->bar);
    }
    return NULL;
}

static int run_stream(size_t arr_mib, int threads, int iters, double delay_s) {
    size_t bytes = arr_mib << 20;
    size_t n = bytes / sizeof(double);
    if (n < 1024) die("--stream-mib too small");
    if (threads <= 0) threads = (int)sysconf(_SC_NPROCESSORS_ONLN);
    if (threads <= 0) threads = 1;

    double t0 = now_s();
    double *a = (double *)alloc_buf(bytes);
    double *b = (double *)alloc_buf(bytes);
    double *c = (double *)alloc_buf(bytes);
    for (size_t i = 0; i < n; i++) { a[i] = 0.0; b[i] = 1.0; c[i] = 2.0; }
    double init_s = now_s() - t0;

    wait_for_window(delay_s, init_s);

    pthread_barrier_t bar;
    if (pthread_barrier_init(&bar, NULL, (unsigned)(threads + 1)) != 0)
        die("pthread_barrier_init failed");
    tri_arg_t *args = (tri_arg_t *)calloc((size_t)threads, sizeof(tri_arg_t));
    pthread_t *tid = (pthread_t *)calloc((size_t)threads, sizeof(pthread_t));
    if (!args || !tid) die("calloc failed");
    size_t chunk = (n + (size_t)threads - 1) / (size_t)threads;
    for (int i = 0; i < threads; i++) {
        args[i].a = a; args[i].b = b; args[i].c = c; args[i].bar = &bar;
        args[i].iters = iters;
        args[i].lo = (size_t)i * chunk;
        args[i].hi = args[i].lo + chunk; if (args[i].hi > n) args[i].hi = n;
        if (args[i].lo > n) args[i].lo = n;
        if (pthread_create(&tid[i], NULL, tri_worker, &args[i]) != 0)
            die("pthread_create failed");
    }
    double best = 1e30;
    for (int it = 0; it < iters; it++) {
        pthread_barrier_wait(&bar);      /* release workers */
        double s = now_s();
        pthread_barrier_wait(&bar);      /* workers finished this iteration */
        double e = now_s() - s;
        if (e < best) best = e;
    }
    for (int i = 0; i < threads; i++) pthread_join(tid[i], NULL);

    /* TWO byte bases, never a bare GB/s (repo doctrine). The 24 B basis counts the
     * architectural traffic (2 reads + 1 write per element); the 32 B basis adds the
     * read-for-ownership of the written line, which is what the DRAM controller
     * actually sees on a machine without non-temporal stores. */
    double b24 = 24.0 * (double)n / best / 1e9;
    double b32 = 32.0 * (double)n / best / 1e9;
    printf("mode=stream\nthreads=%d\narray_bytes=%zu\nelements=%zu\niters=%d\n",
           threads, bytes, n, iters);
    printf("init_s=%.4f\nbest_iter_s=%.6f\ngbps_basis24=%.3f\ngbps_basis32=%.3f\n",
           init_s, best, b24, b32);
    printf("checksum=%.1f\ninit_overrun=0\n", a[0] + a[n - 1]);
    return 0;
}

/* ------------------------------------------------------------------------ main */

static void usage(void) {
    fprintf(stderr,
        "usage: cache-hostile chase  [--buffer-mib N] [--working-kib K]\n"
        "                            [--accesses A] [--seed S] [--arm NAME]\n"
        "                            [--ctl-fifo P --ack-fifo P | --delay-ms D]\n"
        "       cache-hostile stream [--stream-mib N] [--threads T] [--iters I]\n"
        "                            [--delay-ms D]\n"
        "\n"
        "--working-kib 0 means 'the whole buffer' (the cache-hostile arm).\n"
        "--ctl-fifo/--ack-fifo gate the perf window EXACTLY around the chase loop\n"
        "  (use with `perf stat -D -1 --control fifo:<ctl>,<ack>`). This is the\n"
        "  gate mode: it excludes both init AND exit-time teardown.\n"
        "--delay-ms is the fallback when no control FIFO is available; it excludes\n"
        "  init only, and the program exits 4 if init overran the delay.\n");
    exit(2);
}

int main(int argc, char **argv) {
    g_start = now_s();
    if (argc < 2) usage();
    const char *mode = argv[1];
    size_t buf_mib = 2048, working_kib = 0, stream_mib = 512;
    uint64_t accesses = 20000000ULL, seed = 42;
    double delay_s = 10.0;
    int threads = 0, iters = 5;
    const char *arm = "unnamed", *ctl = NULL, *ack = NULL;

    for (int i = 2; i < argc; i++) {
        const char *k = argv[i];
        if (i + 1 >= argc) usage();
        const char *v = argv[++i];
        if (!strcmp(k, "--buffer-mib")) buf_mib = strtoull(v, NULL, 10);
        else if (!strcmp(k, "--working-kib")) working_kib = strtoull(v, NULL, 10);
        else if (!strcmp(k, "--accesses")) accesses = strtoull(v, NULL, 10);
        else if (!strcmp(k, "--delay-ms")) delay_s = strtod(v, NULL) / 1000.0;
        else if (!strcmp(k, "--seed")) seed = strtoull(v, NULL, 10);
        else if (!strcmp(k, "--arm")) arm = v;
        else if (!strcmp(k, "--ctl-fifo")) ctl = v;
        else if (!strcmp(k, "--ack-fifo")) ack = v;
        else if (!strcmp(k, "--stream-mib")) stream_mib = strtoull(v, NULL, 10);
        else if (!strcmp(k, "--threads")) threads = atoi(v);
        else if (!strcmp(k, "--iters")) iters = atoi(v);
        else usage();
    }
    if ((ctl == NULL) != (ack == NULL))
        die("--ctl-fifo and --ack-fifo must be given together");
    if (!strcmp(mode, "chase"))
        return run_chase(buf_mib, working_kib, accesses, delay_s, seed, arm, ctl, ack);
    if (!strcmp(mode, "stream"))
        return run_stream(stream_mib, threads, iters, delay_s);
    usage();
    return 2;
}
