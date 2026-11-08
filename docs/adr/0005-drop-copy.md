# ADR 0005: the drop copy follows the journal instead of living in the engine

**Status:** accepted

## Context

Settlement ([settlement-ledger](https://github.com/davifmn/settlement-ledger)) needs every
trade with the accounts of both sides. The market data stream cannot provide that: its trade
prints are public (no accounts) and a slow subscriber is disconnected, so it may miss trades.
Both are right for market data and wrong for settlement, which must see **every** trade,
**once per id**, in **order** per instrument.

The options:

1. **Publish to Kafka from the engine**, after the journal commit. This puts a network client
   and its failure modes on the matching goroutine, or a queue that must never drop and
   never block. It also adds a third-party module to an engine that has none.
2. **A private execution stream** over HTTP, resumable from a sequence number. The engine
   would have to keep trades in memory for resumption, and decide what happens when a
   consumer asks for something older than that.
3. **Follow the journal.** Every acknowledged command is in the journal (the engine replies
   only after the fsync), and replaying the journal rebuilds exactly the same book. That is
   how restart and the fuzz tests already work. A separate process can read the journal and
   replay it into its own copy of the book, and it sees the same events the engine saw.

## Decision

Option 3, as `cmd/dropcopy`:

- **The engine is untouched on the hot path.** Two small additions:
  - trade events carry the accounts of both sides (`book.Event.Owner`, `MakerOwner`);
  - `engine.Replica` decodes and applies journal records with the engine's own `apply`
    function, so the two cannot drift apart.
- **`journal.ReadFrom` reads without modifying.** It stops at a record that is not complete
  yet (still being written, or a torn tail that matchd cuts off at its next start) and
  resumes there later.
- **Trade ids are deterministic:** `<symbol>-<command seq>-<n>`, where seq is the command's
  position in the journal and n numbers the trades of that command. The same journal always
  gives the same ids, which the ledger deduplicates on.
- **Cursor and delivery.**
  - After Kafka acknowledges a batch (all in-sync replicas, idempotent producer), the drop
    copy saves the last command's seq in a cursor file (write, fsync, rename).
  - On start it replays the whole journal to rebuild the book and publishes only after the
    cursor.
  - A crash between the ack and the cursor publishes that batch again, with the same ids:
    **at least once**, made effectively once by the consumer.
- **Kafka down** means the batch is retried with backoff and the follower waits. Skipping
  would lose trades. `dropcopy_journal_bytes_behind` grows and is the signal to alert on.
- **Order.** Messages are keyed by symbol, so an instrument's trades are on one partition, in
  journal order.
- **Packaging.** `cmd/dropcopy` is its own Go module. The Kafka client (franz-go) is its only
  third-party dependency, and the engine's module keeps none.

## Consequences

- The drop copy needs read access to the journals: a sidecar sharing the volume, or the same
  host. It runs as the same user as matchd, because the journals are created with mode 0600.
- Start-up replays the whole journal. That is the same cost and the same remedy as for
  matchd: snapshots (ADR 0004). The cursor makes sure the replayed trades are not
  published again.
- **A window between write and fsync.** The engine writes a batch, then fsyncs, then replies.
  The drop copy can read the batch from the page cache before the fsync completes. If the
  *machine* loses power in that window, the engine comes back without commands the drop copy
  already published. The client of the order never got an acknowledgement, so the venue
  does not consider them done. Closing the window means the engine publishing its durable
  offset (one more write per commit), or running the drop copy from a replica's journal.
  The window is accepted here and documented.
- **Anonymous trades.** A side with account 0 cannot be settled. Those trades are counted
  (`dropcopy_trades_anonymous_total`), not published, and keep their number so the other
  ids do not shift.
- **A replaced journal** (shorter than the cursor) stops the drop copy with an error instead
  of skipping or repeating trades.
