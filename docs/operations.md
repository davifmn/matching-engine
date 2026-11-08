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

## Drop copy

`dropcopy` (ADR 0005) runs next to matchd with **read-only** access to the journal
directory: a sidecar container sharing the volume, as in `docker-compose.yml`. It runs as the
same user as matchd, because journals are created with mode 0600.

| Flag | Environment | Default | |
|---|---|---|---|
| `-data` | `DROPCOPY_DATA` | `/var/lib/matchd` | matchd's journal directory (read only) |
| `-state` | `DROPCOPY_STATE` | `/var/lib/dropcopy` | cursors, one `<SYMBOL>.cursor` per instrument (keep on a persistent volume) |
| `-instruments` | `DROPCOPY_INSTRUMENTS` | `/etc/matchd/instruments.json` | the same file as matchd |
| `-symbols` | `DROPCOPY_SYMBOLS` | every instrument with a `currency` | instruments to publish |
| `-brokers` | `DROPCOPY_BROKERS` | `localhost:9092` | Kafka bootstrap servers |
| `-topic` | `DROPCOPY_TOPIC` | `trades.executed` | create it beforehand; messages are keyed by symbol |
| `-addr` | `DROPCOPY_ADDR` | `:9100` | `/metrics` and `/healthz` |

| Metric | Meaning |
|---|---|
| `dropcopy_journal_bytes_behind` | journal written by matchd and not yet read: **the** alert (> 0 for minutes = settlement is behind) |
| `dropcopy_publish_failures_total` | failed attempts to publish (each one is retried; Kafka down if it keeps rising) |
| `dropcopy_trades_published_total` | trades published |
| `dropcopy_trades_anonymous_total` | trades not published because a side had no account (should be 0 in a venue where every order has an account) |
| `dropcopy_cursor_seq` | last journal command whose trades are published |

**The drop copy exits with `was the journal replaced?`.** Its cursor says it published trades up
to command N, but the journal now has fewer commands: it was restored from an old backup or
replaced. Publishing on would repeat or skip trade ids. Find out which journal is right,
then either restore it, or, if the new journal is a deliberate fresh start, delete the
instrument's cursor file (the consumer will see the new ids from 1 again; make sure it can
tell them apart, for example with a new symbol).

**`dropcopy_journal_bytes_behind` grows.** Check `dropcopy_publish_failures_total` and the
logs (`publish failed, retrying`): Kafka is unreachable, the topic is missing, or the
producer is not authorized. Nothing is lost while it waits; it catches up by itself.

## Backups

The journal is append-only, so copying it while the server runs is safe. A copy can end
in a torn record, which replay cuts off. Copy every instrument's file, keep the copies off
the host, and test them by starting `matchd -data <copy>` and comparing `/book`.
