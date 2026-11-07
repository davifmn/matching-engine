# ADR 0003: integer ticks and lots, decimal strings on the wire

**Status:** accepted

## Context

Binary floating point cannot represent most decimal prices: `34.15` is stored as
`34.14999999999999857891452847979962825775146484375`. A naive `price / tick` turns it into
3414.999… ticks, and truncation then trades at the wrong price. Floats also make
equality, and so price-time priority, unreliable.

## Decision

- Inside the engine, prices are `int64` **ticks** and quantities `int64` **lots**. An
  instrument's `tick_size` and `lot_size` (`config/instruments.json`) define the units:
  PETR4 trades in R$0.01 ticks and round lots of 100 shares; the mini index future
  WINZ26 trades in ticks of 5 points; BTC-USD trades in lots of 0.0001.
- On the wire, prices and quantities are **decimal strings** (`"34.15"`). JSON numbers are
  refused, since many JSON parsers read them as floats.
- `instrument` converts exactly, with integer arithmetic and overflow checks. A value that
  is not a whole multiple of the increment is **rejected, not rounded**: silently changing
  a price or a quantity is a trading error, while a 400 is a bug the client can fix.

## Consequences

- Matching compares and adds integers only: exact and fast.
- `int64` leaves plenty of room: 9.2 × 10¹⁸ ticks.
- Clients must send values on the grid. The error message says which increment applies.
- Changing an instrument's tick or lot size changes the meaning of the integers in its
  journal. That is a corporate action needing a migration (replay, convert, write a new
  journal), as on real venues.
