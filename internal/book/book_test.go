package book

import (
	"slices"
	"testing"
)

// helpers to keep the scenarios short
func limit(id OrderID, s Side, price, qty int64) Order {
	return Order{ID: id, Side: s, Type: Limit, TIF: GTC, Price: price, Qty: qty}
}

func accepted(o Order) Event {
	return Event{Kind: EventAccepted, Order: o.ID, Side: o.Side, Price: o.Price, Qty: o.Qty, Leaves: o.Qty}
}

func rested(id OrderID, s Side, price, qty int64) Event {
	return Event{Kind: EventRested, Order: id, Side: s, Price: price, Qty: qty, Leaves: qty}
}

func trade(taker OrderID, s Side, price, qty, leaves int64, maker OrderID, makerLeaves int64) Event {
	return Event{Kind: EventTrade, Order: taker, Side: s, Price: price, Qty: qty, Leaves: leaves, Maker: maker, MakerLeaves: makerLeaves}
}

// withAccounts sets the accounts of a trade event: the taker's and the maker's.
func withAccounts(e Event, taker, maker uint64) Event {
	e.Owner, e.MakerOwner = taker, maker
	return e
}

func canceled(id OrderID, s Side, price, qty int64, r Reason) Event {
	return Event{Kind: EventCanceled, Reason: r, Order: id, Side: s, Price: price, Qty: qty}
}

func rejected(o Order, r Reason) Event {
	return Event{Kind: EventRejected, Reason: r, Order: o.ID, Side: o.Side, Price: o.Price, Qty: o.Qty}
}

func mustRest(t *testing.T, b *Book, orders ...Order) {
	t.Helper()
	for _, o := range orders {
		ev := b.Submit(o)
		if last := ev[len(ev)-1]; last.Kind != EventRested {
			t.Fatalf("setup order %d did not rest: %s", o.ID, fmtEvents(ev))
		}
	}
}

func TestRestingOrdersAndBestPrices(t *testing.T) {
	b := New()
	o := limit(1, Buy, 100, 5)
	assertEvents(t, b.Submit(o), accepted(o), rested(1, Buy, 100, 5))
	mustRest(t, b, limit(2, Buy, 101, 1), limit(3, Sell, 103, 2), limit(4, Sell, 102, 7), limit(5, Buy, 100, 2))

	if bid, _ := b.Best(Buy); bid != (PriceLevel{Price: 101, Qty: 1, Orders: 1}) {
		t.Errorf("best bid = %+v", bid)
	}
	if ask, _ := b.Best(Sell); ask != (PriceLevel{Price: 102, Qty: 7, Orders: 1}) {
		t.Errorf("best ask = %+v", ask)
	}
	want := []PriceLevel{{101, 1, 1}, {100, 7, 2}}
	if got := b.Depth(Buy, 0, nil); !slices.Equal(got, want) {
		t.Errorf("bid depth = %v, want %v", got, want)
	}
	if got := b.Depth(Sell, 1, nil); !slices.Equal(got, []PriceLevel{{102, 7, 1}}) {
		t.Errorf("ask depth(1) = %v", got)
	}
	if b.Len() != 5 || b.Levels(Buy) != 2 || b.Levels(Sell) != 2 {
		t.Errorf("Len=%d bids=%d asks=%d", b.Len(), b.Levels(Buy), b.Levels(Sell))
	}
	checkInvariants(t, b)
}

func TestPriceTimePriority(t *testing.T) {
	b := New()
	mustRest(t, b,
		limit(1, Sell, 101, 3), // first at 101
		limit(2, Sell, 101, 3), // second at 101
		limit(3, Sell, 100, 2), // better price, arrived last
	)
	o := limit(4, Buy, 101, 6)
	assertEvents(t, b.Submit(o),
		accepted(o),
		trade(4, Buy, 100, 2, 4, 3, 0), // best price first
		trade(4, Buy, 101, 3, 1, 1, 0), // then the oldest order at 101
		trade(4, Buy, 101, 1, 0, 2, 2),
	)
	if info, ok := b.Lookup(2); !ok || info.Qty != 2 {
		t.Errorf("order 2 = %+v, %v; want 2 lots left", info, ok)
	}
	checkInvariants(t, b)
}

func TestTradesAtMakerPriceAndRestsRemainder(t *testing.T) {
	b := New()
	mustRest(t, b, limit(1, Sell, 100, 4))
	o := limit(2, Buy, 105, 10)
	assertEvents(t, b.Submit(o),
		accepted(o),
		trade(2, Buy, 100, 4, 6, 1, 0), // price improvement: the maker's price
		rested(2, Buy, 105, 6),
	)
	if _, ok := b.Best(Sell); ok {
		t.Error("ask side should be empty")
	}
	checkInvariants(t, b)
}

func TestLimitPriceStopsTheSweep(t *testing.T) {
	b := New()
	mustRest(t, b, limit(1, Buy, 99, 5), limit(2, Buy, 98, 5))
	o := limit(3, Sell, 99, 8)
	assertEvents(t, b.Submit(o), accepted(o), trade(3, Sell, 99, 5, 3, 1, 0), rested(3, Sell, 99, 3))
	checkInvariants(t, b)
}

func TestMarketOrders(t *testing.T) {
	t.Run("sweeps levels and cancels what is left", func(t *testing.T) {
		b := New()
		mustRest(t, b, limit(1, Sell, 100, 2), limit(2, Sell, 150, 2))
		o := Order{ID: 3, Side: Buy, Type: Market, TIF: IOC, Qty: 5}
		assertEvents(t, b.Submit(o),
			accepted(o),
			trade(3, Buy, 100, 2, 3, 1, 0),
			trade(3, Buy, 150, 2, 1, 2, 0),
			canceled(3, Buy, 0, 1, ReasonNoLiquidity),
		)
		checkInvariants(t, b)
	})
	t.Run("on an empty book", func(t *testing.T) {
		o := Order{ID: 1, Side: Sell, Type: Market, TIF: IOC, Qty: 5}
		assertEvents(t, New().Submit(o), accepted(o), canceled(1, Sell, 0, 5, ReasonNoLiquidity))
	})
	t.Run("price is ignored", func(t *testing.T) {
		b := New()
		mustRest(t, b, limit(1, Sell, 100, 2))
		o := Order{ID: 2, Side: Buy, Type: Market, TIF: IOC, Price: 1, Qty: 2}
		ev := b.Submit(o)
		if ev[1].Kind != EventTrade || ev[1].Price != 100 {
			t.Fatalf("market order with a price did not trade: %s", fmtEvents(ev))
		}
	})
}

func TestImmediateOrCancel(t *testing.T) {
	b := New()
	mustRest(t, b, limit(1, Sell, 100, 2))
	o := Order{ID: 2, Side: Buy, Type: Limit, TIF: IOC, Price: 100, Qty: 5}
	assertEvents(t, b.Submit(o), accepted(o), trade(2, Buy, 100, 2, 3, 1, 0), canceled(2, Buy, 100, 3, ReasonNoLiquidity))
	if b.Len() != 0 {
		t.Error("IOC remainder must not rest")
	}
}

func TestFillOrKill(t *testing.T) {
	setup := func(t *testing.T) *Book {
		b := New()
		mustRest(t, b, limit(1, Sell, 100, 3), limit(2, Sell, 101, 3), limit(3, Sell, 103, 10))
		return b
	}
	t.Run("not enough at acceptable prices: rejected, book untouched", func(t *testing.T) {
		b := setup(t)
		before := b.Digest()
		o := Order{ID: 9, Side: Buy, Type: Limit, TIF: FOK, Price: 101, Qty: 7}
		assertEvents(t, b.Submit(o), rejected(o, ReasonFOKNotFilled))
		if b.Digest() != before || len(b.Changes()) != 0 {
			t.Error("a rejected FOK changed the book")
		}
	})
	t.Run("exactly enough: fills across levels", func(t *testing.T) {
		b := setup(t)
		o := Order{ID: 9, Side: Buy, Type: Limit, TIF: FOK, Price: 101, Qty: 6}
		assertEvents(t, b.Submit(o), accepted(o), trade(9, Buy, 100, 3, 3, 1, 0), trade(9, Buy, 101, 3, 0, 2, 0))
	})
	t.Run("market FOK", func(t *testing.T) {
		b := setup(t)
		o := Order{ID: 9, Side: Buy, Type: Market, TIF: FOK, Qty: 17}
		if ev := b.Submit(o); ev[0].Kind != EventRejected {
			t.Fatalf("FOK for more than the book holds: %s", fmtEvents(ev))
		}
		o.Qty = 16
		if ev := b.Submit(o); ev[len(ev)-1].Leaves != 0 || b.Len() != 0 {
			t.Fatalf("FOK for the whole book: %s", fmtEvents(ev))
		}
	})
}

func TestPostOnly(t *testing.T) {
	b := New()
	mustRest(t, b, limit(1, Sell, 101, 3))
	cross := Order{ID: 2, Side: Buy, Type: Limit, TIF: GTC, Price: 101, Qty: 1, PostOnly: true}
	assertEvents(t, b.Submit(cross), rejected(cross, ReasonPostOnlyWouldCross))
	passive := cross
	passive.Price = 100
	assertEvents(t, b.Submit(passive), accepted(passive), rested(2, Buy, 100, 1))
}

func TestSelfTradePrevention(t *testing.T) {
	// Resting asks: 10 (owner 7) at 100, then 11 (owner 8) at 100.
	setup := func(t *testing.T) *Book {
		b := New()
		a := limit(10, Sell, 100, 2)
		a.Owner = 7
		c := limit(11, Sell, 100, 2)
		c.Owner = 8
		mustRest(t, b, a, c)
		return b
	}
	taker := func(stp STP) Order {
		o := limit(20, Buy, 100, 3)
		o.Owner, o.STP = 7, stp
		return o
	}
	t.Run("cancel newest", func(t *testing.T) {
		b := setup(t)
		o := taker(CancelNewest)
		assertEvents(t, b.Submit(o), accepted(o), canceled(20, Buy, 100, 3, ReasonSelfTrade))
		if b.Len() != 2 {
			t.Error("resting orders must stay")
		}
	})
	t.Run("cancel oldest", func(t *testing.T) {
		b := setup(t)
		o := taker(CancelOldest)
		assertEvents(t, b.Submit(o),
			accepted(o),
			canceled(10, Sell, 100, 2, ReasonSelfTrade),
			withAccounts(trade(20, Buy, 100, 2, 1, 11, 0), 7, 8),
			rested(20, Buy, 100, 1),
		)
		checkInvariants(t, b)
	})
	t.Run("cancel both", func(t *testing.T) {
		b := setup(t)
		o := taker(CancelBoth)
		assertEvents(t, b.Submit(o), accepted(o), canceled(10, Sell, 100, 2, ReasonSelfTrade), canceled(20, Buy, 100, 3, ReasonSelfTrade))
		if _, ok := b.Lookup(11); !ok || b.Len() != 1 {
			t.Error("only the self order should be canceled")
		}
	})
	t.Run("anonymous orders never trigger STP", func(t *testing.T) {
		b := New()
		mustRest(t, b, limit(1, Sell, 100, 1))
		o := limit(2, Buy, 100, 1)
		assertEvents(t, b.Submit(o), accepted(o), trade(2, Buy, 100, 1, 0, 1, 0))
	})
	t.Run("FOK counts the queue the way STP would match it", func(t *testing.T) {
		o := taker(CancelNewest)
		o.ID, o.TIF, o.Qty = 21, FOK, 2
		b := setup(t)
		assertEvents(t, b.Submit(o), rejected(o, ReasonFOKNotFilled)) // own order is first in line
		o.STP = CancelOldest
		ev := b.Submit(o) // skips (cancels) its own order and fills against order 11
		if ev[len(ev)-1].Kind != EventTrade || ev[len(ev)-1].Leaves != 0 {
			t.Fatalf("FOK with cancel_oldest: %s", fmtEvents(ev))
		}
	})
}

func TestCancel(t *testing.T) {
	b := New()
	mustRest(t, b, limit(1, Buy, 100, 5), limit(2, Buy, 100, 1))
	assertEvents(t, b.Cancel(1), canceled(1, Buy, 100, 5, ReasonCanceledByUser))
	assertEvents(t, b.Cancel(1), Event{Kind: EventRejected, Reason: ReasonUnknownOrder, Order: 1})
	if lvl, _ := b.Best(Buy); lvl != (PriceLevel{100, 1, 1}) {
		t.Errorf("level after cancel = %+v", lvl)
	}
	assertEvents(t, b.Cancel(2), canceled(2, Buy, 100, 1, ReasonCanceledByUser))
	if b.Levels(Buy) != 0 {
		t.Error("empty level not removed")
	}
	checkInvariants(t, b)
}

func TestModify(t *testing.T) {
	modified := func(id OrderID, s Side, price, qty int64) Event {
		return Event{Kind: EventModified, Order: id, Side: s, Price: price, Qty: qty, Leaves: qty}
	}
	queue := func(b *Book) (ids []OrderID) {
		for o := range b.Orders(Buy) {
			ids = append(ids, o.ID)
		}
		return ids
	}
	setup := func(t *testing.T) *Book {
		b := New()
		mustRest(t, b, limit(1, Buy, 100, 5), limit(2, Buy, 100, 5), limit(3, Sell, 103, 4))
		return b
	}
	t.Run("lower quantity keeps the place in the queue", func(t *testing.T) {
		b := setup(t)
		assertEvents(t, b.Modify(1, 100, 2), modified(1, Buy, 100, 2))
		if got := queue(b); !slices.Equal(got, []OrderID{1, 2}) {
			t.Errorf("queue = %v", got)
		}
		if lvl, _ := b.Best(Buy); lvl.Qty != 7 {
			t.Errorf("level qty = %d, want 7", lvl.Qty)
		}
	})
	t.Run("higher quantity goes to the back", func(t *testing.T) {
		b := setup(t)
		assertEvents(t, b.Modify(1, 100, 6), modified(1, Buy, 100, 6), rested(1, Buy, 100, 6))
		if got := queue(b); !slices.Equal(got, []OrderID{2, 1}) {
			t.Errorf("queue = %v", got)
		}
	})
	t.Run("new price that crosses trades at once", func(t *testing.T) {
		b := setup(t)
		assertEvents(t, b.Modify(2, 103, 5),
			modified(2, Buy, 103, 5),
			trade(2, Buy, 103, 4, 1, 3, 0),
			rested(2, Buy, 103, 1),
		)
		checkInvariants(t, b)
	})
	t.Run("post-only order that would cross keeps its old terms", func(t *testing.T) {
		b := setup(t)
		p := limit(4, Buy, 99, 1)
		p.PostOnly = true
		mustRest(t, b, p)
		assertEvents(t, b.Modify(4, 103, 1), Event{Kind: EventRejected, Reason: ReasonPostOnlyWouldCross, Order: 4, Side: Buy, Price: 103, Qty: 1})
		if info, _ := b.Lookup(4); info.Price != 99 {
			t.Errorf("post-only order moved to %d", info.Price)
		}
	})
	t.Run("invalid", func(t *testing.T) {
		b := setup(t)
		assertEvents(t, b.Modify(9, 100, 1), Event{Kind: EventRejected, Reason: ReasonUnknownOrder, Order: 9, Price: 100, Qty: 1})
		assertEvents(t, b.Modify(1, 100, 0), Event{Kind: EventRejected, Reason: ReasonInvalidQty, Order: 1, Side: Buy, Price: 100})
		assertEvents(t, b.Modify(1, -1, 1), Event{Kind: EventRejected, Reason: ReasonInvalidPrice, Order: 1, Side: Buy, Price: -1, Qty: 1})
	})
}

func TestValidation(t *testing.T) {
	ok := limit(1, Buy, 100, 1)
	cases := []struct {
		name   string
		change func(*Order)
		want   Reason
	}{
		{"zero id", func(o *Order) { o.ID = 0 }, ReasonInvalidID},
		{"no side", func(o *Order) { o.Side = 0 }, ReasonInvalidSide},
		{"bad type", func(o *Order) { o.Type = 9 }, ReasonInvalidType},
		{"no time in force", func(o *Order) { o.TIF = 0 }, ReasonInvalidTIF},
		{"market GTC", func(o *Order) { o.Type = Market }, ReasonInvalidTIF},
		{"post-only IOC", func(o *Order) { o.PostOnly, o.TIF = true, IOC }, ReasonInvalidTIF},
		{"bad STP", func(o *Order) { o.STP = 9 }, ReasonInvalidSTP},
		{"zero price", func(o *Order) { o.Price = 0 }, ReasonInvalidPrice},
		{"negative quantity", func(o *Order) { o.Qty = -1 }, ReasonInvalidQty},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			o := ok
			c.change(&o)
			assertEvents(t, New().Submit(o), rejected(o, c.want))
		})
	}
	t.Run("duplicate id", func(t *testing.T) {
		b := New()
		mustRest(t, b, ok)
		assertEvents(t, b.Submit(ok), rejected(ok, ReasonDuplicateID))
	})
}

func TestChanges(t *testing.T) {
	b := New()
	mustRest(t, b, limit(1, Sell, 101, 3), limit(2, Sell, 102, 3), limit(3, Buy, 99, 1))
	b.Submit(limit(4, Buy, 102, 5)) // empties 101, takes 2 at 102, nothing left to rest
	want := []LevelChange{{Sell, 101, 0}, {Sell, 102, 1}}
	if got := b.Changes(); !slices.Equal(got, want) {
		t.Errorf("changes = %v, want %v", got, want)
	}
	b.Submit(limit(5, Buy, 99, 2))
	if got := b.Changes(); !slices.Equal(got, []LevelChange{{Buy, 99, 3}}) {
		t.Errorf("changes = %v", got)
	}
	b.Cancel(42)
	if got := b.Changes(); len(got) != 0 {
		t.Errorf("a rejected cancel reported changes: %v", got)
	}
}

func TestDigestDependsOnQueueOrder(t *testing.T) {
	a, b := New(), New()
	mustRest(t, a, limit(1, Buy, 100, 1), limit(2, Buy, 100, 1))
	mustRest(t, b, limit(2, Buy, 100, 1), limit(1, Buy, 100, 1))
	if a.Digest() == b.Digest() {
		t.Error("books with different queue order have the same digest")
	}
	a.Cancel(1)
	b.Cancel(1)
	if a.Digest() != b.Digest() {
		t.Error("equal books have different digests")
	}
}

// After warm-up the matching path reuses orders, levels and the event buffer.
func TestNoAllocationsInSteadyState(t *testing.T) {
	b := New()
	mustRest(t, b, limit(1, Sell, 105, 10), limit(2, Buy, 95, 10))
	id := OrderID(100)
	cycle := func() {
		id++
		b.Submit(limit(id, Buy, 100, 3)) // rests
		b.Modify(id, 100, 2)             // reduce in place
		b.Submit(Order{ID: id + 1_000_000, Side: Sell, Type: Limit, TIF: IOC, Price: 100, Qty: 1})
		b.Cancel(id)
		b.Changes()
	}
	for range 100 {
		cycle()
	}
	if allocs := testing.AllocsPerRun(1000, cycle); allocs > 0 {
		t.Errorf("%.1f allocations per cycle, want 0", allocs)
	}
}

func TestTradesCarryTheAccountsOfBothSides(t *testing.T) {
	b := New()
	b.Submit(Order{ID: 1, Owner: 1001, Side: Buy, Type: Limit, TIF: GTC, Price: 100, Qty: 5})
	b.Submit(Order{ID: 2, Owner: 1002, Side: Buy, Type: Limit, TIF: GTC, Price: 99, Qty: 5})

	ev := b.Submit(Order{ID: 3, Owner: 2001, Side: Sell, Type: Market, TIF: IOC, Qty: 7})

	assertEvents(t, ev,
		Event{Kind: EventAccepted, Order: 3, Side: Sell, Qty: 7, Leaves: 7},
		withAccounts(trade(3, Sell, 100, 5, 2, 1, 0), 2001, 1001),
		withAccounts(trade(3, Sell, 99, 2, 0, 2, 3), 2001, 1002))

	// A modify that crosses makes the modified order the taker, with its own account.
	b.Submit(Order{ID: 4, Owner: 3001, Side: Sell, Type: Limit, TIF: GTC, Price: 105, Qty: 1})
	for _, e := range b.Modify(4, 99, 1) {
		if e.Kind == EventTrade && (e.Owner != 3001 || e.MakerOwner != 1002) {
			t.Fatalf("modify trade accounts = %d/%d, want 3001/1002", e.Owner, e.MakerOwner)
		}
	}
}
