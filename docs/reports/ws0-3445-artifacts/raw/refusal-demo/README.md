# `refusal-demo/` — evidence that a REFUSED rep actually refuses

Captured with `--secs 8 --settle 3 --max-load 0.01` on a box at loadavg ~59. The point it
demonstrates: the harness **exits 3** and leaves its artifacts in place, rather than writing a
`REFUSED` verdict file and then returning success — the defect roborev found in round 3.

```
verdict=REFUSED
reason=box-not-quiet-across-rep
samples=2
peak_load=59.21
max_load=0.01
```

## These exact parameters are now rejected EARLIER, and that is a stronger guard

`--secs 8` with the default `--load-sample-secs 5` would now fail argument validation, because
round 4 added the rule that the sampling interval must be under HALF the measurement duration —
otherwise the sampler observes only the window's endpoints, which is the very read the
across-window sampler replaced. This capture (`samples=2`) is precisely that endpoint-only shape,
so it is also a demonstration of why the later rule was needed.

The verdict it demonstrates is unaffected: a peak above the bound still yields
`verdict=REFUSED` + exit 3. Retained as-is rather than re-captured, because re-capturing needs a
measurement rep and the corpus (see `../validity-and-refusals.md` for why none were taken).
