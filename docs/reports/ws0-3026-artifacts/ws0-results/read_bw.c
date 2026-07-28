/*
 * ws0-readbw.c - pure sequential READ bandwidth, CQLite issue #3026 (WS0).
 *
 * WHY THIS EXISTS SEPARATELY: a naive `sum += a[i]` over doubles is NOT a
 * bandwidth measurement. GCC will not vectorize an FP-add reduction (floating
 * point addition isn't associative, so reassociation is illegal without
 * -ffast-math), leaving the loop bound by the ~4-cycle serial FP-add dependency
 * chain. Our first attempt measured 7.45 GB/s that way -- a latency artifact,
 * not DRAM bandwidth. Here we use uint64 with 8 INDEPENDENT accumulators, which
 * vectorizes and keeps enough memory-level parallelism in flight to be genuinely
 * bandwidth-bound. Reads only => no write-allocate traffic to reason about, so
 * counted bytes == actual DRAM bytes.
 *
 * Build: gcc -O3 -march=native -fopenmp -o ws0-readbw read_bw.c
 * Run:   ./ws0-readbw <MiB-total> <ntimes>
 */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <time.h>
#include <omp.h>

static double mysecond(void) {
    struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + 1.0e-9 * (double)ts.tv_nsec;
}

int main(int argc, char **argv) {
    size_t mib = (argc > 1) ? (size_t)strtoull(argv[1], NULL, 10) : 2048;
    int ntimes = (argc > 2) ? atoi(argv[2]) : 10;
    size_t bytes = mib * 1024ULL * 1024ULL;
    size_t n = bytes / sizeof(uint64_t);

    uint64_t *a = NULL;
    if (posix_memalign((void **)&a, 4096, bytes)) { fprintf(stderr,"alloc fail\n"); return 1; }

    int nthreads = 0;
    #pragma omp parallel
    { 
        #pragma omp master
        nthreads = omp_get_num_threads(); 
    }

    #pragma omp parallel for schedule(static)
    for (size_t i = 0; i < n; i++) a[i] = (uint64_t)i * 2654435761ULL;

    double best = 1.0e30;
    uint64_t guard = 0;
    for (int it = 0; it < ntimes; it++) {
        uint64_t total = 0;
        double t = mysecond();
        #pragma omp parallel for schedule(static) reduction(+:total)
        for (size_t chunk = 0; chunk < n; chunk += 8) {
            /* 8 independent adds -> vectorizable, plenty of MLP */
            total += a[chunk] + a[chunk+1] + a[chunk+2] + a[chunk+3]
                   + a[chunk+4] + a[chunk+5] + a[chunk+6] + a[chunk+7];
        }
        t = mysecond() - t;
        if (t < best) best = t;
        guard ^= total;
    }

    printf("# ws0-readbw threads=%d buffer=%zu MiB (%.2f GiB, %.1fx the 105 MiB L3) ntimes=%d\n",
           nthreads, mib, (double)bytes/(1024.0*1024.0*1024.0),
           (double)bytes/(105.0*1024.0*1024.0), ntimes);
    printf("READ %8.2f GB/s(1e9)  %8.2f GiB/s  best %.6f s  guard %llu\n",
           (double)bytes/best/1.0e9, (double)bytes/best/(1024.0*1024.0*1024.0),
           best, (unsigned long long)guard);
    free(a);
    return 0;
}
