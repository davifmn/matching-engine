package book

import (
	"fmt"
	"slices"
	"testing"
)

// checkInvariants walks the internal structure and fails the test if anything is
// inconsistent: level order, list links, cached totals, the ID index, a crossed book.
func checkInvariants(t testing.TB, b *Book) {
	t.Helper()
	if err := invariantError(b); err != nil {
		t.Fatal(err)
	}
}

func invariantError(b *Book) error {
	seen := 0
	for _, s := range []*bookSide{&b.bids, &b.asks} {
		for i, lvl := range s.levels {
			if i > 0 && s.rank(s.levels[i-1].price) >= s.rank(lvl.price) {
				return fmt.Errorf("%s levels out of order at %d: %d then %d", s.side, i, s.levels[i-1].price, lvl.price)
			}
			if lvl.n <= 0 || lvl.head == nil || lvl.tail == nil {
				return fmt.Errorf("%s level %d is empty but still on the book", s.side, lvl.price)
			}
			if lvl.head.prev != nil || lvl.tail.next != nil {
				return fmt.Errorf("%s level %d: broken list ends", s.side, lvl.price)
			}
			var n int
			var qty int64
			var prev *order
			for m := lvl.head; m != nil; m = m.next {
				switch {
				case m.prev != prev:
					return fmt.Errorf("order %d: prev link broken", m.id)
				case m.lvl != lvl || m.price != lvl.price || m.side != s.side:
					return fmt.Errorf("order %d: wrong level, price or side", m.id)
				case m.qty <= 0:
					return fmt.Errorf("order %d rests with qty %d", m.id, m.qty)
				case b.orders[m.id] != m:
					return fmt.Errorf("order %d missing from the index", m.id)
				}
				n++
				qty += m.qty
				prev = m
			}
			if prev != lvl.tail {
				return fmt.Errorf("%s level %d: tail is not the last order", s.side, lvl.price)
			}
			if n != lvl.n || qty != lvl.qty {
				return fmt.Errorf("%s level %d: cached n=%d qty=%d, actual n=%d qty=%d", s.side, lvl.price, lvl.n, lvl.qty, n, qty)
			}
			seen += n
		}
	}
	if seen != len(b.orders) {
		return fmt.Errorf("index has %d orders, levels hold %d", len(b.orders), seen)
	}
	bid, okB := b.Best(Buy)
	ask, okA := b.Best(Sell)
	if okB && okA && bid.Price >= ask.Price {
		return fmt.Errorf("crossed book: bid %d >= ask %d", bid.Price, ask.Price)
	}
	return nil
}

func assertEvents(t testing.TB, got []Event, want ...Event) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Fatalf("events differ\n got: %s\nwant: %s", fmtEvents(got), fmtEvents(want))
	}
}

func fmtEvents(es []Event) string {
	s := ""
	for _, e := range es {
		s += "\n  " + e.String()
	}
	if s == "" {
		return " (none)"
	}
	return s
}
