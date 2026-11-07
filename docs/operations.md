# Operations

## Configuration

| Flag | Environment | Default | |
|---|---|---|---|
| `-addr` | `MATCHD_ADDR` | `:8080` | listen address |
| `-data` | `MATCHD_DATA` | `data` | journal directory (one `<SYMBOL>.wal` per instrument); empty means in memory only |
| `-instruments` | `MATCHD_INSTRUMENTS` | `config/instruments.json` | instrument definitions |
| `-sync` | `MATCHD_SYNC` | `commit` | `commit`: fsync once per batch. `none`: let the OS flush (a machine crash can lose the last commands; only for tests or a replica) |
| `-log-level` | `MATCHD_LOG_LEVEL` | `info` | `debug` adds one access log line per request |

Run it as a single replica per set of instruments (a StatefulSet with one pod and a
persistent volume, for example). Two processes must never write the same journal.

## Endpoints

| Endpoint | |
|---|---|
| `GET /healthz` | liveness: the process is up |
| `GET /readyz` | readiness: 503 if any instrument stopped |
| `GET /metrics` | Prometheus text format |

## Metrics and suggested alerts

| Metric | Meaning |
|---|---|
| `matching_command_seconds` | enqueue → reply, per instrument: queueing + journal + matching |
| `matching_journal_commit_seconds` | write + fsync per batch |
| `matching_batch_commands` | commands per batch (group commit size) |
| `matching_commands_total`, `matching_rejects_total{reason}` | traffic and rejections |
| `matching_trades_total`, `matching_traded_lots_total` | activity |
| `matching_resting_orders`, `matching_journal_bytes` | book and journal size |
| `matching_market_data_subscribers`, `matching_slow_consumers_total` | market data |
| `http_requests_total{route,code}`, `http_request_duration_seconds{route}` | HTTP |

```promql
# An instrument stopped (journal failure): page.
max(probe_success{job="matchd-readyz"}) == 0

# p99 latency of the engine above 50 ms for 5 minutes: disk trouble, usually.
histogram_quantile(0.99, sum by (le, instrument) (rate(matching_command_seconds_bucket[5m]))) > 0.05

# fsync getting slow.
histogram_quantile(0.99, sum by (le) (rate(matching_journal_commit_seconds_bucket[5m]))) > 0.02

# Unusual rejection rate (a client bug or an attack).
sum by (instrument) (rate(matching_rejects_total[5m])) / sum by (instrument) (rate(matching_commands_total[5m])) > 0.2
```

## Disk usage and start-up time

Each command takes 56 bytes in the journal (8 of framing plus 48). Ten million commands are
about 560 MB. Replay runs at millions of commands per second; the start-up log line
`journal replayed` reports the count and the time taken. Snapshots with journal rotation
are the planned fix once replay gets too slow (ADR 0004).

## Runbook

**`/readyz` returns 503, logs say `journal failed, stopping the instrument`.**
The disk returned an error on write or fsync. The instrument stopped without acknowledging
the failed batch, so the clients of that batch got 503 and must check their orders.
1. Fix the disk (full? read-only remount? `dmesg`).
2. Restart the process. Replay rebuilds the book; a torn last record is cut off
   automatically (`removed a torn record` warning).

**The process exits at start-up with `journal: corrupt record at offset N`.**
A damaged record is followed by valid data: this is not a crash artifact, so nothing is
cut automatically.
1. Keep a copy of the file: `cp PETR4.wal PETR4.wal.corrupt`.
2. Restore the journal from backup or from a replica, if there is one.
3. With no other copy, `truncate -s N PETR4.wal` keeps every command before the damage and
   loses the rest. The commands after N were acknowledged, so reconcile with the members'
   records (drop copy) before reopening.

**`matching_slow_consumers_total` keeps increasing.**
Market data clients are not keeping up, usually a slow network path or a client doing
work on its read loop. They are disconnected, so matching is unaffected; tell the client.

**Latency spikes with no traffic change.**
Check `matching_journal_commit_seconds` (disk) against `matching_batch_commands` (load).
Noisy neighbours on shared disks are the usual cause. Use a dedicated volume.

## Backups

The journal is append-only, so copying it while the server runs is safe. A copy can end
in a torn record, which replay cuts off. Copy every instrument's file, keep the copies off
the host, and test them by starting `matchd -data <copy>` and comparing `/book`.
