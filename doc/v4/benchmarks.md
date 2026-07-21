# Carmine v3/v4 benchmark baseline

Run the opt-in same-process comparison with a local Redis server:

```bash
lein bench-v3-v4
```

The runner reports median nanoseconds per operation. It compares the v3 default
pool with the v4 default pool and a v4 pool whose validation checks are disabled
to match v3's default borrow policy. Absolute times depend on the machine. Use
the same-host `v4-comparable/v3` ratio. Configure the run with
`CARMINE_BENCH_WARMUP_LAPS`, `CARMINE_BENCH_TIMED_LAPS`, and
`CARMINE_BENCH_SAMPLES`. With an even sample count, the runner reports the upper
middle sample; prefer an odd count.

## Current baseline

Captured 2026-07-13 on a 2020 Apple M1 MacBook Pro, OpenJDK 25, and local
Redis 8 (1,000 warm-up laps, 5,000 timed laps per sample, five samples):

| Operation | v3 default | v4 comparable | v4 default | v4 comparable / v3 | v4 default / v3 |
|---|---:|---:|---:|---:|---:|
| Empty `wcar` | 2,677 ns | 2,312 ns | 2,563 ns | 0.864 | 0.957 |
| `PING` | 75,247 ns | 73,038 ns | 74,153 ns | 0.971 | 0.985 |
| `SET` + `GET` pipeline | 78,277 ns | 77,311 ns | 77,671 ns | 0.988 | 0.992 |
| 100-command `PING` pipeline | 184,886 ns | 188,983 ns | 189,059 ns | 1.022 | 1.023 |

The v4 default manager validates connections when it creates or borrows them
and while they are idle. It skips `PING` while a connection has been idle for
less than `:ready-check-after-idle-ms` (default 5000), so hot-pool results are
close to the validation-disabled results. Longer idle periods get the liveness
check.
