/*
 * ws0-stream.c - STREAM-style memory bandwidth benchmark for CQLite issue #3026 (WS0).
 *
 * Classic STREAM kernels (Copy/Scale/Add/Triad) per McCalpin, with:
 *   - array size selectable at runtime (must be >> L3; this box has 105 MiB L3)
 *   - OpenMP threading; thread pinning is done EXTERNALLY via taskset so we can
 *     pick distinct PHYSICAL cores (logical 0-7 on this 16-thread/8-core box).
 *   - reports best-of-N GB/s per kernel using 1e9 bytes/s ("GB/s" decimal, the
 *     STREAM convention) plus GiB/s for cross-checking.
 *
 * Build: gcc -O3 -march=native -fopenmp -o ws0-stream stream_bench.c
 * Run:   ./ws0-stream <MiB-per-array> <ntimes>
 *
 * Bytes counted per kernel (STREAM convention, counts compulsory traffic only;
 * NOTE: on a write-allocate machine without non-temporal stores the true DRAM
 * traffic is higher because stores incur a read-for-ownership. We report the
 * STREAM convention and flag this explicitly in the writeup.)
 */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <omp.h>
#include <sched.h>

typedef double STREAM_TYPE;

static double mysecond(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + 1.0e-9 * (double)ts.tv_nsec;
}

int main(int argc, char **argv) {
    size_t mib_per_array = (argc > 1) ? (size_t)strtoull(argv[1], NULL, 10) : 512;
    int ntimes = (argc > 2) ? atoi(argv[2]) : 10;
    if (ntimes < 3) ntimes = 3;

    size_t bytes_per_array = mib_per_array * 1024ULL * 1024ULL;
    size_t n = bytes_per_array / sizeof(STREAM_TYPE);

    STREAM_TYPE *a = NULL, *b = NULL, *c = NULL;
    if (posix_memalign((void **)&a, 4096, bytes_per_array) ||
        posix_memalign((void **)&b, 4096, bytes_per_array) ||
        posix_memalign((void **)&c, 4096, bytes_per_array)) {
        fprintf(stderr, "alloc failed\n");
        return 1;
    }

    int nthreads = 0;
    #pragma omp parallel
    { 
        #pragma omp master
        nthreads = omp_get_num_threads();
    }

    /* First-touch init in parallel so pages land near the touching thread. */
    #pragma omp parallel for schedule(static)
    for (size_t i = 0; i < n; i++) { a[i] = 1.0; b[i] = 2.0; c[i] = 0.0; }

    const char *label[5] = {"Copy", "Scale", "Add", "Triad", "Read"};
    /* words moved per element, STREAM convention */
    double wpe[5] = {2.0, 2.0, 3.0, 3.0, 1.0};
    volatile double sink = 0.0;
    double best[5]; for (int k = 0; k < 5; k++) best[k] = 1.0e30;
    STREAM_TYPE scalar = 3.0;

    for (int it = 0; it < ntimes; it++) {
        double t;

        t = mysecond();
        #pragma omp parallel for schedule(static)
        for (size_t i = 0; i < n; i++) c[i] = a[i];
        t = mysecond() - t; if (t < best[0]) best[0] = t;

        t = mysecond();
        #pragma omp parallel for schedule(static)
        for (size_t i = 0; i < n; i++) b[i] = scalar * c[i];
        t = mysecond() - t; if (t < best[1]) best[1] = t;

        t = mysecond();
        #pragma omp parallel for schedule(static)
        for (size_t i = 0; i < n; i++) c[i] = a[i] + b[i];
        t = mysecond() - t; if (t < best[2]) best[2] = t;

        t = mysecond();
        #pragma omp parallel for schedule(static)
        for (size_t i = 0; i < n; i++) a[i] = b[i] + scalar * c[i];
        t = mysecond() - t; if (t < best[3]) best[3] = t;

        /* Pure-read kernel: streaming reduction, no stores -> no write-allocate
         * traffic. This is the cleanest "how fast can one core pull bytes from
         * DRAM" number, and is the closest analogue to CQLite scanning row bytes. */
        double rsum = 0.0;
        t = mysecond();
        #pragma omp parallel for schedule(static) reduction(+:rsum)
        for (size_t i = 0; i < n; i++) rsum += a[i];
        t = mysecond() - t; if (t < best[4]) best[4] = t;
        sink = rsum;
    }

    /* consume results so nothing is optimized away */
    double sum = 0.0;
    for (size_t i = 0; i < n; i += (n / 97 + 1)) sum += a[i] + b[i] + c[i];

    printf("# ws0-stream  threads=%d  array=%zu MiB each (3 arrays, %.2f GiB total)  n=%zu  ntimes=%d\n",
           nthreads, mib_per_array, (3.0 * (double)bytes_per_array) / (1024.0*1024.0*1024.0), n, ntimes);
    printf("# L3 on this box = 105 MiB; per-array footprint is %.1fx L3\n",
           (double)bytes_per_array / (105.0*1024.0*1024.0));
    printf("%-8s %12s %12s %12s\n", "Kernel", "GB/s(1e9)", "GiB/s", "best_s");
    for (int k = 0; k < 5; k++) {
        double b_moved = wpe[k] * (double)sizeof(STREAM_TYPE) * (double)n;
        printf("%-8s %12.2f %12.2f %12.6f\n", label[k],
               b_moved / best[k] / 1.0e9,
               b_moved / best[k] / (1024.0*1024.0*1024.0),
               best[k]);
    }
    printf("# checksum %.6e sink %.6e\n", sum, (double)sink);
    free(a); free(b); free(c);
    return 0;
}
