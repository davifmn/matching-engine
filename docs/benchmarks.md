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

Results from the GitHub Actions runner are added here after the first CI run.
