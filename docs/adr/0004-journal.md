# ADR 0004: a write-ahead journal of commands, with group commit

**Status:** accepted

## Context

An acknowledged order must survive a crash: if the client got `201 resting`, the order
must still be on the book after a restart. The options:

1. **Apply, reply, write in the background.** Fast, but a crash loses acknowledged orders.
2. **Write and fsync each command before applying it.** Durable, but an fsync takes from
   ~0.1 ms (a datacenter NVMe) to several ms (a laptop SSD with a full flush), which caps
   an instrument at a few hundred to a few thousand commands per second.
3. **Group commit.** The shard takes every request waiting in its queue (up to 256),
   writes all of their commands with one `write` and one `fsync`, and only then applies
   them and replies. It is as durable as option 2, and its throughput grows with
   concurrency: one fsync is shared by every command that arrived while the previous one
   ran.

What to journal:

- **State.** Simple to reload, but every change rewrites a lot.
- **Commands (inputs).** Small (48 bytes each), append-only. Needs deterministic matching.

## Decision

Group commit (option 3), journaling commands. The matching path is deterministic: no
clocks, no randomness, no map iteration order. The engine assigns the order ID and the
sequence number *before* writing, and the timestamp is journaled for audit but never read
by matching. So replaying the journal rebuilds the same book, queue positions included.
`Book.Digest()` hashes the full state, which lets tests (and, later, replicas) check this.

Each record is `length | CRC-32C | payload`. On start-up:

- a missing file is an empty book;
- an incomplete last record, or a last record whose checksum fails, is what a crash in the
  middle of a write leaves behind: it was never acknowledged, so it is truncated and the
  start-up goes on (logged as a warning);
- a bad record followed by more data is real corruption. The instrument **refuses to
  start**, because cutting there would silently drop acknowledged commands. See
  `docs/operations.md`.

At runtime, if a write or fsync fails, the instrument **stops** (fail-stop): pending
requests get 503, and `/readyz` fails. It never acknowledges a command it could not make
durable. Other instruments keep running.

## Consequences

- Durable acknowledgements at thousands of commands per second, even with a slow fsync.
  Locally `loadgen` shows how batches grow with concurrency; `matching_batch_commands`
  exports the batch sizes.
- A command whose write failed has an unknown outcome: the bytes may or may not have
  reached the disk. The API says so in the 503 body, and clients must check before
  retrying (`GET /v1/instruments/{symbol}/orders/{id}`).
- Replay time grows with history (millions of commands per second, so about a second per
  million). Next steps, not built yet: periodic snapshots with journal rotation, and a
  follower that tails the journal and checks its digest against the leader.
