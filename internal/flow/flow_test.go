package flow

import (
	"testing"

	"github.com/davifmn/matching-engine/internal/book"
)

func TestDeterministicPerSeed(t *testing.T) {
	a, b, c := New(7, 1000, DefaultMix), New(7, 1000, DefaultMix), New(8, 1000, DefaultMix)
	same := true
	for range 1000 {
		x, y, z := a.Next(10), b.Next(10), c.Next(10)
		if x != y {
			t.Fatal("same seed, different flow")
		}
		same = same && x == z
	}
	if same {
		t.Fatal("different seeds, same flow")
	}
}

func TestMixAndPrices(t *testing.T) {
	g := New(1, 1000, DefaultMix)
	counts := map[Kind]int{}
	const n = 100_000
	for range n {
		op := g.Next(100)
		counts[op.Kind]++
		switch op.Kind {
		case Passive, Aggressive:
			if op.Price < 1 || op.Qty < 1 || op.Qty > 99 {
				t.Fatalf("bad order %+v", op)
			}
			mid := g.Mid()
			passive := (op.Side == book.Buy && op.Price < mid) || (op.Side == book.Sell && op.Price > mid)
			if (op.Kind == Passive) != passive {
				t.Fatalf("%s order on the wrong side of mid %d: %+v", op.Kind, mid, op)
			}
		case Cancel, Modify:
			if op.Pick < 0 || op.Pick >= 100 {
				t.Fatalf("pick out of range: %+v", op)
			}
		}
	}
	for k, w := range map[Kind]int{Passive: 55, Aggressive: 10, Market: 5, Cancel: 25, Modify: 5} {
		if got := counts[k] * 100 / n; got < w-2 || got > w+2 {
			t.Errorf("%s: %d%%, want about %d%%", k, got, w)
		}
	}
	if g := New(1, 1000, DefaultMix); g.Next(0).Kind == Cancel {
		t.Error("cancel generated with no live orders")
	}
}

func TestLive(t *testing.T) {
	l := NewLive()
	l.Apply([]book.Event{
		{Kind: book.EventRested, Order: 1},
		{Kind: book.EventRested, Order: 2},
		{Kind: book.EventRested, Order: 3},
		{Kind: book.EventRested, Order: 3}, // idempotent
	})
	l.Apply([]book.Event{{Kind: book.EventTrade, Order: 9, Leaves: 0, Maker: 1, MakerLeaves: 0}})
	l.Apply([]book.Event{{Kind: book.EventCanceled, Order: 2}, {Kind: book.EventCanceled, Order: 42}})
	if l.Len() != 1 || l.At(0) != 3 {
		t.Fatalf("live = %v", l.ids)
	}
	l.Remove(3)
	l.Remove(3)
	if l.Len() != 0 {
		t.Fatal("not empty")
	}
}
