# ADR 0001: one goroutine owns each order book

**Status:** accepted

## Context

Every order reads and changes the book of its instrument. Many HTTP handlers run at the
same time, so something has to serialize access. The options:

1. **A mutex around the book.** Simple, but every handler contends for the lock. The
   critical section is short (about 140 ns per command, see `docs/benchmarks.md`), so
   under contention the lock hand-offs can cost more than the matching they protect, and
   there is no natural point to batch journal writes.
2. **Lock-free structures.** A concurrent skip list or tree. Hard to get right, hard to
   test, and matching needs several structures (levels, queues, ID index) to change
   atomically together, which lock-free designs cannot give cheaply.
3. **A single writer.** One goroutine owns the book and is the only code that touches it;
   everyone else sends it messages. This is the single-writer principle behind the LMAX
   exchange's architecture and most production matching engines.

## Decision

Option 3. `engine` starts one goroutine per instrument (a *shard*). Requests reach it
through a bounded channel; replies go back on a per-request channel.

## Consequences

- `book` has no locks and no goroutines. It is plain sequential code: deterministic, easy
  to fuzz against a reference model, and allocation-free in steady state.
- Determinism makes recovery simple: replaying the same commands rebuilds the same book
  (ADR 0004).
- The queue is a natural place to batch: the shard drains what is waiting and commits it
  to the journal in one fsync.
- The queue also gives backpressure: when it is full, callers wait (or give up when their
  request context ends) instead of piling work onto the book.
- Instruments run in parallel on different cores; one instrument is limited to one core.
  That is the norm on real venues, which shard by instrument. The book alone handles
  millions of commands per second, so the limit is the journal and the network first.
- A channel hand-off wakes a goroutine, which costs a few microseconds of latency. Engines
  that chase nanoseconds busy-spin on ring buffers pinned to isolated cores. That trade-off
  is not worth it for a portable service on shared machines, and it can be revisited behind
  the same `engine` API.
