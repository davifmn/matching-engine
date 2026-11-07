# Benchmarks

## What is measured

| Benchmark | Measures |
|---|---|
| `book` `BenchmarkRealisticFlow` | One command of a recorded flow of 500,000: 55% passive limit orders, 10% aggressive limit orders, 5% market orders, 25% cancels, 5% modifies, around a random-walking mid price (`internal/flow`). This is the number that matters. |
| `book` `BenchmarkRealisticFlowLatency` | The same replay, timing each command: p50, p99, p99.9 |
| `book` `BenchmarkRestAndCancel` | Add a passive order inside a 1,000-level book, then cancel it |
| `book` `BenchmarkTakeTopOfBook` | An IOC order trading with the best resting order |
| `book` `BenchmarkSweepTenLevels` | Rest 40 orders on 10 levels, then sweep them with one market order |
| `engine` `BenchmarkEngine/*` | One command through the whole engine (queue, journal, matching, reply) from 16 × GOMAXPROCS concurrent clients: in memory, journal without fsync, journal with fsync |
| `cmd/loadgen` | Over HTTP: real requests from concurrent clients, latency per operation |

Run them with `make bench` (and `benchstat` to compare runs). CI runs them on every push
and puts a `benchstat` summary in the run's summary page.

## Results

GitHub Actions `ubuntu-latest` runner: Intel Xeon Platinum 8370C, **2 vCPUs**, Linux, Go
stable. Medians of 6 runs (CI run of 2026-11-07). Shared CI machines are noisy, so treat
these as orders of magnitude; compare your own runs with `benchstat`.

### The book alone

| Benchmark | Time per op | Allocations |
|---|---|---|
| Realistic flow (mixed commands) | **136 ns** (about 7 million commands/s) | 0 |
| Realistic flow, per-command latency | p50 **144 ns**, p99 **360 ns**, p99.9 **788 ns** | 0 |
| Add and cancel in a 1,000-level book | 119 ns for the pair | 0 |
| Take the top of the book (IOC trade) | 49 ns | 0 |
| Rest 40 orders on 10 levels, then sweep them | 5.0 µs (~120 ns per order) | 0 |

### Through the engine (queue, journal, matching, reply), 32 concurrent clients

| Mode | Time per command | Commands/s |
|---|---|---|
| In memory | 1.2 µs | ~830,000 |
| Journal, no fsync | 1.2 µs | ~850,000 |
| Journal, fsync per batch (durable) | 12.2 µs | ~82,000 |

The channel hand-off between goroutines (ADR 0001) dominates without fsync: the book's own
136 ns is a tenth of it. With fsync, one flush takes a few hundred microseconds on the
runner's disk, and group commit shares it among every command waiting at the time.
Fsyncing each command separately would cap the instrument at a few thousand commands/s.

### Over HTTP: `scripts/e2e.sh` in CI

16 `loadgen` clients for 15 s, server and load generator on the same 2 vCPUs, journal with
fsync per batch:

| Operation | Count | p50 | p90 | p99 | p99.9 |
|---|---|---|---|---|---|
| all | 175,525 (**11,700 req/s**, 0 failures) | 1.15 ms | 1.80 ms | 3.87 ms | 11.9 ms |

The worst request took 714 ms. Every operation type shows the same maximum, so it was one
stall that hit all clients at once; most likely the shared runner's disk or scheduler,
though it was not investigated further. HTTP and JSON
cost far more than matching. The 404s in the run are cancels and modifies of orders that
other clients had already filled, which is expected.

### On a laptop

On a MacBook (Apple M2; the run used an x86 Go toolchain under Rosetta, so CPU numbers are
pessimistic) the same HTTP run gives about 2,600 req/s with a p50 of 6 ms. macOS `fsync`
flushes the drive cache (`F_FULLFSYNC`), which takes milliseconds, so latency is the
fsync and throughput comes from batching 16 clients per flush. Run with `-sync none` to see
the difference.
