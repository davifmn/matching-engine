# Architecture

```
 clients ──HTTP/JSON──▶ api ──────────────┐            ┌───────────── shard: PETR4 (one goroutine) ─────────────┐
 (orders, cancels,      decimal → ticks   │  bounded   │ 1. take up to 256 queued requests                      │
  modifies, book)       validation        ├─ queue ───▶│ 2. assign order IDs + sequence numbers                 │
                        status mapping    │  per       │ 3. journal: one write + one fsync for the whole batch  │──▶ data/PETR4.wal
                                          │  instrument│ 4. apply each command to the book (no locks)           │
 subscribers ◀──SSE──── stream ◀──────────┘            │ 5. reply to each caller; publish level changes+trades  │
 (snapshot + updates)   slow consumer = disconnect     └────────────────────────────────────────────────────────┘
                                                        (WINZ26, BTC-USD: same, in parallel)
```

## Packages

| Package | Role | Depends on |
|---|---|---|
| `internal/book` | The order book: price-time priority, limit/market, GTC/IOC/FOK, post-only, self-trade prevention, cancel, modify. Sequential, deterministic, zero allocations in steady state. | – |
| `internal/engine` | One goroutine per instrument; group commit to the journal; replay on start; market data fan-out; metrics. | book, journal, metrics |
| `internal/journal` | Append-only log with CRC-32C framing, torn-tail recovery, corruption detection. | – |
| `internal/instrument` | Exact decimal ⇄ tick/lot conversion; instrument definitions. | – |
| `internal/api` | HTTP handlers, SSE stream, request metrics, access logs. | engine, instrument, metrics |
| `internal/metrics` | Prometheus text-format counters, gauges and histograms. | – |
| `internal/flow`, `internal/latency` | Synthetic order flow and a latency histogram for benchmarks, tests and `loadgen`. | book |
| `cmd/matchd` | The server: flags/env config, graceful shutdown. | all of the above |
| `cmd/loadgen` | HTTP load generator with per-operation latency percentiles. | flow, latency, instrument |
| `internal/dropcopy` | Follows an instrument's journal (`journal.ReadFrom`), replays it with `engine.Replica`, turns trade events into settlement messages, keeps a cursor. | engine, journal, instrument, metrics |
| `cmd/dropcopy` | The drop copy process: publishes to Kafka. A separate Go module, so the engine keeps no dependencies. | dropcopy, franz-go |

Only the standard library is used, so there is nothing to audit or update but Go itself.
`govulncheck` runs in CI.

## Life of an order

1. `POST /v1/instruments/PETR4/orders` with `{"side":"buy","type":"limit","price":"34.15","quantity":"300"}`.
2. `api` parses the JSON (unknown fields and bodies over 4 KB are rejected), converts
   `34.15` into 3415 ticks and `300` into 3 lots, and calls `engine.Submit`.
3. The request waits in the PETR4 queue. If the queue is full, the handler waits until
   the request context ends, which is the backpressure.
4. The PETR4 goroutine takes it with whatever else is queued, gives it ID 1042 and
   sequence 98,311, and writes the batch to `PETR4.wal` with one fsync.
5. `book.Submit` matches it: best price first, oldest order first within a price, at the
   resting order's price. The rest goes on the book (GTC) or is canceled (IOC/market).
   The result is a list of events: `accepted`, `trade`…, `rested` or `canceled`.
6. The goroutine replies to the handler and publishes the changed levels and the trades
   to subscribers, each with the next market data sequence number.
7. `api` turns the events into `{"status":"resting","filled_quantity":"100","remaining_quantity":"200","fills":[…]}`.

## Matching rules

| Rule | Behaviour |
|---|---|
| Priority | Best price, then arrival time. A trade happens at the resting order's price. |
| Limit / market | Market orders take any price and never rest; they must be IOC or FOK. |
| GTC / IOC / FOK | GTC rests the remainder. IOC cancels it. FOK fills completely at once or is rejected without touching the book. |
| Post-only | A GTC limit order that would take liquidity is rejected instead. |
| Self-trade prevention | Orders of the same `account_id` never trade with each other. The incoming order's mode decides: `cancel_newest` (default) cancels the incoming remainder, `cancel_oldest` cancels the resting order and keeps matching, `cancel_both` does both. FOK accounts for this exactly. |
| Modify | Lowering the quantity at the same price keeps the order's place in the queue. Any other change loses it, and a new price that crosses trades at once (or is rejected if post-only, keeping the old terms). |

## Failure handling

| Failure | What happens |
|---|---|
| Process crash, `kill -9`, power loss | Every acknowledged command is in the journal. Replay rebuilds the same book; a torn last record (never acknowledged) is truncated. CI runs this (`scripts/e2e.sh`). |
| Journal write/fsync fails | That instrument stops; its requests get 503 with "outcome unknown"; `/readyz` fails so the orchestrator stops sending traffic. Other instruments keep running. |
| Corrupt journal (not a torn tail) | That instrument refuses to start: see `docs/operations.md`. |
| Slow market data client | Its buffer (1,024 messages) fills, it gets an `error` event and is disconnected. Matching never waits for a consumer. |
| Client gap in market data | Each update's `seq` is the previous one + 1; on a gap, the client reconnects and gets a fresh snapshot. |
| SIGTERM | Stop accepting connections, end streams, finish queued requests, close journals, exit 0. |

## Out of scope (and what a real venue adds)

- **Authentication and authorization.** `account_id` comes from the request body here;
  in production it comes from the authenticated session (mTLS or signed requests).
- **Pre-trade risk.** Credit limits, price collars, maximum order size, kill switches.
  These sit between `api` and `engine`.
- **Order history.** Only resting orders can be queried. The drop copy (ADR 0005) publishes
  every trade with both accounts; a full order history (fills, cancels, modifies per order)
  would come from the same journal follower, written to a database.
- **Snapshots and replication.** See ADR 0004: periodic snapshots bound replay time; a
  follower replaying the leader's journal (and comparing `Digest`) gives a hot standby.
- **Binary protocols.** HTTP/JSON keeps the project approachable. Exchanges use FIX or
  binary protocols (ITCH/OUCH-style) over TCP, or multicast for market data. The `engine`
  API is transport-agnostic.
