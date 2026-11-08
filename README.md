# matching-engine

[![CI](https://github.com/davifmn/matching-engine/actions/workflows/ci.yml/badge.svg)](https://github.com/davifmn/matching-engine/actions/workflows/ci.yml)

A limit order book matching engine in Go, built the way a trading venue builds one:
price-time priority, one single-threaded book per instrument, a write-ahead journal with
group commit, crash recovery by deterministic replay, and incremental market data with
sequence numbers. It uses only the standard library.

Its trades are settled by [settlement-ledger](https://github.com/davifmn/settlement-ledger):
a **drop copy** follows the engine's journal and publishes every trade, with the accounts
of both sides, to Kafka ([ADR 0005](docs/adr/0005-drop-copy.md)).

- **Order types:** limit and market orders; GTC, IOC and FOK; post-only; cancel and modify
  (a quantity reduction keeps queue priority); self-trade prevention in three modes.
- **Fast:** the book makes zero allocations in steady state and takes 136 ns per command
  of a realistic order flow (p99 360 ns). The whole engine with a durable journal handles
  about 80,000 commands/s on a 2-vCPU CI runner ([benchmarks](docs/benchmarks.md)).
- **Durable:** an order is acknowledged only after its batch is fsynced. A restart, or a
  `kill -9` under load, rebuilds the exact same book. CI checks both.
- **Exact:** prices and quantities are integer ticks and lots, with decimal strings on the
  wire, so no floating point touches a price.
- **Checked against a model:** a fuzz test runs random command sequences on the engine and
  on a deliberately naive reference book and requires identical events, event by event.
- **Operable:** Prometheus metrics, JSON logs, health and readiness probes, graceful
  shutdown, a distroless container, a [runbook](docs/operations.md).
- **Settled downstream:** the drop copy replays the journal into its own copy of the book,
  so it sees exactly the engine's trades, with ids that survive any restart. CI kills each
  process with `kill -9` and checks that no trade is missing on the topic.

```
 HTTP/JSON ─▶ api ─▶ queue ─▶ ┌ shard PETR4 (one goroutine) ─────────────────┐
                              │ batch ─▶ journal (1 write + 1 fsync) ─▶ book │ ─▶ replies
 SSE ◀── market data ◀────────┤ level changes + trades, seq numbered         │
                              └───────────────────┬──────────────────────────┘
                                                  │ PETR4.wal (read only)
 settlement-ledger ◀── Kafka trades.executed ◀── dropcopy: replays the journal into its own book,
                       (key = symbol)            publishes each trade with both accounts, then
                                                 saves its cursor (at least once, stable ids)
```

See **[docs/architecture.md](docs/architecture.md)** for the life of an order, the
matching rules and the failure model, and the ADRs for the main decisions:
[single writer](docs/adr/0001-single-writer.md) ·
[price levels](docs/adr/0002-price-levels.md) ·
[fixed point](docs/adr/0003-fixed-point.md) ·
[journal and group commit](docs/adr/0004-journal.md) ·
[drop copy](docs/adr/0005-drop-copy.md).

## Quick start

Requires Go 1.24 or newer.

```sh
make run        # builds bin/matchd and bin/loadgen, serves on :8080 with journals in ./data
```

Instruments come from [`config/instruments.json`](config/instruments.json): PETR4 (ticks
of R$0.01, round lots of 100), WINZ26 (mini index future, ticks of 5 points) and BTC-USD
(lots of 0.0001).

```sh
# a resting sell
curl -s localhost:8080/v1/instruments/PETR4/orders \
  -d '{"side":"sell","type":"limit","price":"34.15","quantity":"500","account_id":1}'
# {"order_id":"1","seq":1,"status":"resting","price":"34.15","filled_quantity":"0","remaining_quantity":"500","fills":[]}

# a buy that crosses: it trades at the resting price, 34.15
curl -s localhost:8080/v1/instruments/PETR4/orders \
  -d '{"side":"buy","type":"limit","price":"34.20","quantity":"300","account_id":2}'
# {"order_id":"2","seq":2,"status":"filled","filled_quantity":"300","remaining_quantity":"0",
#  "fills":[{"price":"34.15","quantity":"300","maker_order_id":"1"}]}

curl -s 'localhost:8080/v1/instruments/PETR4/book?depth=5'
curl -s -X PATCH localhost:8080/v1/instruments/PETR4/orders/1 -d '{"price":"34.10","quantity":"100"}'
curl -s -X DELETE localhost:8080/v1/instruments/PETR4/orders/1
curl -N localhost:8080/v1/instruments/PETR4/stream     # market data as Server-Sent Events

bin/loadgen -symbol PETR4 -mid 34.00 -clients 16 -duration 10s
```

## API

| Method and path | |
|---|---|
| `GET /v1/instruments` | instruments with tick and lot sizes |
| `POST /v1/instruments/{symbol}/orders` | new order: `side`, `type`, `price` (limit), `quantity`, optional `time_in_force` (`gtc`/`ioc`/`fok`), `post_only`, `account_id`, `self_trade_prevention` (`cancel_newest`/`cancel_oldest`/`cancel_both`) |
| `GET /v1/instruments/{symbol}/orders/{id}` | a resting order |
| `PATCH /v1/instruments/{symbol}/orders/{id}` | new `price` and open `quantity` |
| `DELETE /v1/instruments/{symbol}/orders/{id}` | cancel |
| `GET /v1/instruments/{symbol}/book?depth=N` | aggregated levels, with the sequence number they reflect |
| `GET /v1/instruments/{symbol}/stream` | SSE: one `snapshot`, then an `update` per change (`seq` increments by 1; on a gap, reconnect) |
| `GET /healthz`, `GET /readyz`, `GET /metrics` | probes and Prometheus metrics |

Order responses carry `status` (`resting`, `filled`, `canceled`, `rejected`), the
`reason` for cancels and rejections, `filled_quantity`, `remaining_quantity` and the
`fills`. HTTP status: 201/200 when accepted; 400 for malformed input (including a price
off the tick grid, which is never rounded); 404 for an unknown instrument or order; 422
when the book refuses the order (post-only would cross, FOK cannot fill); 503 when the
instrument is unavailable, in which case the outcome is unknown and the order should be
checked before retrying.

## Performance

Medians on a 2-vCPU GitHub Actions runner ([details and method](docs/benchmarks.md)):

| | |
|---|---|
| Book, realistic mix of commands | 136 ns per command, 0 allocations; p50 144 ns, p99 360 ns |
| Take the top of the book | 49 ns |
| Engine, journal fsynced per batch, 32 clients | 12 µs per command (~82,000/s) |
| HTTP, 16 clients, durable journal | 11,700 req/s; p50 1.2 ms, p99 3.9 ms |

## How it is tested

| Layer | What |
|---|---|
| Matching rules | Table-driven unit tests for priority, every order type, STP modes, modify semantics, validation and incremental level changes |
| Model-based | `FuzzAgainstReference` decodes random bytes into commands and runs them on the book and on `refBook` (a slice and a linear scan, obviously correct); events, resting orders, internal invariants and the depth rebuilt from incremental changes must all match. CI fuzzes for 90 s on every push; `go test` alone runs 200 seeded scripts |
| Allocation | `TestNoAllocationsInSteadyState` fails if the matching path allocates |
| Engine | Restart rebuilds the same book (same digest, IDs and sequence); torn writes are truncated; a corrupt journal refuses to start; a failing disk stops only that instrument; concurrent clients; market data rebuilds the book with no sequence gaps; slow consumers are dropped |
| Journal | Every possible cut point of the last record, garbled tails, corruption in the middle |
| HTTP | Order lifecycle, rejections and status codes, input validation, SSE stream, metrics |
| Binary | `cmd/matchd` test: start, trade, SIGTERM, restart, identical book |
| End to end | `scripts/e2e.sh` in CI: 15 s of `loadgen`, graceful restart, `kill -9` under load, recovery, identical book |
| Drop copy | A replica fed with the journal returns exactly the engine's events and digest over 3,000 commands. `journal.ReadFrom` at every cut point of a record being written: it waits and never modifies the file. The follower against an in-process engine: ids equal the engine's; a restart publishes only what is new; a crash before the cursor is saved republishes with the same ids; it keeps up across an engine restart; a broker outage delays but loses nothing; a replaced journal stops it |
| Drop copy end to end | `scripts/dropcopy_e2e.sh` in CI, with docker compose and Kafka: `kill -9` of the drop copy and later of matchd during trading, then every trade id the engine reported must be on the topic, none missing and none invented |
| Container | Docker build, smoke test across `docker stop`/`start`, Trivy scan of both images |
| Supply chain | `go vet`, staticcheck, govulncheck; no third-party modules in the engine (the drop copy's module has one: the Kafka client) |

```sh
make race    # all tests with the race detector
make fuzz    # FUZZTIME=10m for a longer run
make bench   # COUNT=10 for steadier numbers
make e2e
make lint vuln
```

## Repository layout

```
cmd/matchd        server: flags/env config, graceful shutdown
cmd/loadgen       HTTP load generator with latency percentiles
cmd/dropcopy      drop copy to Kafka (its own Go module: the only third-party dependency)
internal/dropcopy journal follower, trade messages, cursor (no Kafka code)
internal/book     the order book (no I/O, no goroutines, no allocations)
internal/engine   per-instrument goroutines, group commit, replay, market data
internal/journal  checksummed append-only log
internal/instrument  exact decimal ⇄ ticks/lots
internal/api      HTTP handlers and SSE
internal/metrics  Prometheus text exposition
internal/flow     synthetic order flow; internal/latency: percentile histogram
docs/             architecture, ADRs, benchmarks, operations
scripts/e2e.sh    restart and crash recovery test of the real binaries
scripts/dropcopy_e2e.sh  matchd + dropcopy + Kafka with kill -9 of each (docker compose)
```

## License

MIT
