// Calibration kernel for memory-traffic counters (issue #3026 WS0).
// Streams a buffer LARGER than L3 (105 MiB on this SPR box) so every read
// must come from DRAM. Known ground truth: bytes_read = BUF * PASSES.
// Usage: membw_cal <buf_mib> <passes> [rw]
//   rw absent  -> read-only  (sum)
//   rw present -> read+write (buf[i] += 1)  => also generates writeback traffic
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <time.h>

int main(int argc, char **argv) {
    size_t mib = (argc > 1) ? strtoull(argv[1], 0, 10) : 512;
    size_t passes = (argc > 2) ? strtoull(argv[2], 0, 10) : 8;
    int rw = (argc > 3);
    size_t n = mib * 1024ull * 1024ull;
    uint64_t *b = aligned_alloc(4096, n);
    if (!b) { perror("alloc"); return 1; }
    memset(b, 1, n);
    size_t words = n / sizeof(uint64_t);
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    uint64_t s = 0;
    for (size_t p = 0; p < passes; p++) {
        if (rw) { for (size_t i = 0; i < words; i += 8) b[i] += 1; s += b[0]; }
        else    { for (size_t i = 0; i < words; i += 8) s += b[i]; }
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);
    double sec = (t1.tv_sec - t0.tv_sec) + (t1.tv_nsec - t0.tv_nsec) / 1e9;
    double bytes = (double)n * (double)passes;
    fprintf(stderr, "ground_truth_bytes_touched=%.0f (%.2f GiB) sec=%.4f GB/s=%.2f sink=%llu\n",
            bytes, bytes / (1024.0*1024*1024), sec, bytes / sec / 1e9,
            (unsigned long long)s);
    return 0;
}
