# ADR 0002: price levels in a sorted slice, orders in intrusive FIFO lists

**Status:** accepted

## Context

The book needs, per side:

- the best price, on every incoming order;
- removal of the best level when it empties, which is frequent;
- insertion of a new level, usually within a few ticks of the best;
- cancellation of any order by ID, about a quarter of all messages on a real venue;
- FIFO order within a price (time priority).

## Options for the levels

| Structure | Best price | Insert / remove level | Notes |
|---|---|---|---|
| Balanced tree (red-black, B-tree) | O(log n), or O(1) with a cached pointer | O(log n) | General, but pointer-heavy: cache misses on every step |
| Heap | O(1) | O(log n), and removing a non-top level is awkward | Poor fit for cancels deep in the book |
| Array indexed by tick | O(1) via a tracked best index | O(1) | Fastest, but needs a bounded price band; sparse books waste memory, and finding the next best level is a scan |
| **Sorted slice, best level last** | **O(1)** | **O(1) at the top; O(k) memmove for a level k positions from the top; binary search to find it** | Contiguous, cache-friendly, simple |

## Decision

- Each side is a slice of `*level` sorted from the worst price to the best, so the best
  level is the last element. Reading it, and removing it when it empties, are O(1). A new
  level near the top shifts only the few pointers above it. Deeper levels are found by
  binary search, after a fast-path check against the top.
- Each level holds an intrusive doubly linked list of its orders (FIFO), plus cached
  totals. A map from order ID to the order node lets cancel and modify unlink in O(1).
- Order and level nodes are recycled through free lists, and the event buffer is reused,
  so the steady-state matching path makes **zero allocations** (enforced by
  `TestNoAllocationsInSteadyState` and visible as `0 allocs/op` in the benchmarks). The
  garbage collector has nothing to do because of matching.

## Consequences

- Fast where real flow concentrates: `BenchmarkTakeTopOfBook` and the realistic flow mix.
- Worst case: a new level at the far end of a 1,000-level side moves 8 KB of pointers,
  roughly 100 ns. `BenchmarkRestAndCancel` measures inserts within a 1,000-level book.
- If an instrument has very wide, sparse books (options chains, long-dated futures), a
  B-tree, or a tick-indexed array with a price band, would be better. The structure is
  private to `book`; the fuzz test against the reference model would check any
  replacement.
