package book

import (
	"cmp"
	"maps"
	"math/rand/v2"
	"slices"
	"testing"
)

// refBook is a deliberately naive order book: one unsorted slice, a linear scan for the
// best order on every step, and FOK decided by trying the match on a copy. It is slow and
// obviously correct, and the fuzz test checks that Book produces exactly the same events.
// Only the stateless validation (validate) is shared with Book.
type refBook struct {
	orders []refOrder
	seq    int
	ev     []Event
}

type refOrder struct {
	OrderInfo
	seq int // arrival order, for time priority
}

func (r *refBook) clone() *refBook {
	return &refBook{orders: slices.Clone(r.orders), seq: r.seq}
}

func (r *refBook) find(id OrderID) int {
	return slices.IndexFunc(r.orders, func(o refOrder) bool { return o.ID == id })
}

// best returns the index of the order with the highest priority on side s, or -1.
func (r *refBook) best(s Side) int {
	bi := -1
	for i, o := range r.orders {
		if o.Side != s {
			continue
		}
		if bi < 0 {
			bi = i
			continue
		}
		b := r.orders[bi]
		better := (s == Buy && o.Price > b.Price) || (s == Sell && o.Price < b.Price)
		if better || (o.Price == b.Price && o.seq < b.seq) {
			bi = i
		}
	}
	return bi
}

func (r *refBook) emit(e Event) { r.ev = append(r.ev, e) }

func (r *refBook) reject(id OrderID, s Side, price, qty int64, reason Reason) []Event {
	r.emit(Event{Kind: EventRejected, Reason: reason, Order: id, Side: s, Price: price, Qty: qty})
	return r.ev
}

func (r *refBook) submit(o Order) []Event {
	r.ev = nil
	if reason := validate(&o); reason != ReasonNone {
		return r.reject(o.ID, o.Side, o.Price, o.Qty, reason)
	}
	if r.find(o.ID) >= 0 {
		return r.reject(o.ID, o.Side, o.Price, o.Qty, ReasonDuplicateID)
	}
	if o.Type == Market {
		o.Price = 0
	}
	if o.PostOnly {
		if i := r.best(o.Side.Opposite()); i >= 0 && acceptable(o.Side, o.Price, r.orders[i].Price) {
			return r.reject(o.ID, o.Side, o.Price, o.Qty, ReasonPostOnlyWouldCross)
		}
	}
	if o.TIF == FOK {
		trial, c := r.clone(), o
		trial.match(&c)
		if c.Qty > 0 {
			return r.reject(o.ID, o.Side, o.Price, o.Qty, ReasonFOKNotFilled)
		}
	}
	r.emit(Event{Kind: EventAccepted, Order: o.ID, Side: o.Side, Price: o.Price, Qty: o.Qty, Leaves: o.Qty})
	r.finish(&o, r.match(&o))
	return r.ev
}

func (r *refBook) match(o *Order) (stopped bool) {
	for o.Qty > 0 {
		i := r.best(o.Side.Opposite())
		if i < 0 || (o.Type == Limit && !acceptable(o.Side, o.Price, r.orders[i].Price)) {
			return false
		}
		m := &r.orders[i]
		if o.Owner != 0 && m.Owner == o.Owner {
			if o.STP == CancelNewest {
				return true
			}
			r.cancelAt(i, ReasonSelfTrade)
			if o.STP == CancelBoth {
				return true
			}
			continue
		}
		q := min(o.Qty, m.Qty)
		o.Qty -= q
		m.Qty -= q
		r.emit(Event{Kind: EventTrade, Order: o.ID, Side: o.Side, Price: m.Price, Qty: q, Leaves: o.Qty, Maker: m.ID, MakerLeaves: m.Qty, Owner: o.Owner, MakerOwner: m.Owner})
		if m.Qty == 0 {
			r.orders = slices.Delete(r.orders, i, i+1)
		}
	}
	return false
}

func (r *refBook) finish(o *Order, stopped bool) {
	switch {
	case o.Qty == 0:
	case stopped:
		r.emit(Event{Kind: EventCanceled, Reason: ReasonSelfTrade, Order: o.ID, Side: o.Side, Price: o.Price, Qty: o.Qty})
	case o.Type == Limit && o.TIF == GTC:
		r.seq++
		r.orders = append(r.orders, refOrder{
			OrderInfo: OrderInfo{ID: o.ID, Owner: o.Owner, Side: o.Side, Price: o.Price, Qty: o.Qty, PostOnly: o.PostOnly, STP: o.STP},
			seq:       r.seq,
		})
		r.emit(Event{Kind: EventRested, Order: o.ID, Side: o.Side, Price: o.Price, Qty: o.Qty, Leaves: o.Qty})
	default:
		r.emit(Event{Kind: EventCanceled, Reason: ReasonNoLiquidity, Order: o.ID, Side: o.Side, Price: o.Price, Qty: o.Qty})
	}
}

func (r *refBook) cancelAt(i int, reason Reason) {
	m := r.orders[i]
	r.emit(Event{Kind: EventCanceled, Reason: reason, Order: m.ID, Side: m.Side, Price: m.Price, Qty: m.Qty})
	r.orders = slices.Delete(r.orders, i, i+1)
}

func (r *refBook) cancel(id OrderID) []Event {
	r.ev = nil
	i := r.find(id)
	if i < 0 {
		return r.reject(id, 0, 0, 0, ReasonUnknownOrder)
	}
	r.cancelAt(i, ReasonCanceledByUser)
	return r.ev
}

func (r *refBook) modify(id OrderID, price, qty int64) []Event {
	r.ev = nil
	i := r.find(id)
	if i < 0 {
		return r.reject(id, 0, price, qty, ReasonUnknownOrder)
	}
	m := r.orders[i]
	switch {
	case qty <= 0:
		return r.reject(id, m.Side, price, qty, ReasonInvalidQty)
	case price <= 0:
		return r.reject(id, m.Side, price, qty, ReasonInvalidPrice)
	}
	if price == m.Price && qty <= m.Qty {
		r.orders[i].Qty = qty // keeps its seq, so it keeps its place
		r.emit(Event{Kind: EventModified, Order: id, Side: m.Side, Price: price, Qty: qty, Leaves: qty})
		return r.ev
	}
	if m.PostOnly {
		if j := r.best(m.Side.Opposite()); j >= 0 && acceptable(m.Side, price, r.orders[j].Price) {
			return r.reject(id, m.Side, price, qty, ReasonPostOnlyWouldCross)
		}
	}
	r.orders = slices.Delete(r.orders, i, i+1)
	r.emit(Event{Kind: EventModified, Order: id, Side: m.Side, Price: price, Qty: qty, Leaves: qty})
	o := Order{ID: id, Owner: m.Owner, Side: m.Side, Type: Limit, TIF: GTC, Price: price, Qty: qty, PostOnly: m.PostOnly, STP: m.STP}
	r.finish(&o, r.match(&o))
	return r.ev
}

// sorted returns the orders of one side in priority order, like Book.Orders.
func (r *refBook) sorted(s Side) []OrderInfo {
	var os []refOrder
	for _, o := range r.orders {
		if o.Side == s {
			os = append(os, o)
		}
	}
	slices.SortFunc(os, func(a, b refOrder) int {
		if a.Price != b.Price {
			if s == Buy {
				return cmp.Compare(b.Price, a.Price)
			}
			return cmp.Compare(a.Price, b.Price)
		}
		return cmp.Compare(a.seq, b.seq)
	})
	out := make([]OrderInfo, len(os))
	for i, o := range os {
		out[i] = o.OrderInfo
	}
	return out
}

type levelKey struct {
	side  Side
	price int64
}

func (r *refBook) depth() map[levelKey]int64 {
	d := map[levelKey]int64{}
	for _, o := range r.orders {
		d[levelKey{o.Side, o.Price}] += o.Qty
	}
	return d
}

// pick chooses a live order ID from a byte, or now and then an ID that does not exist.
func (r *refBook) pick(sel byte, unknown OrderID) OrderID {
	if len(r.orders) == 0 || sel%8 == 7 {
		return unknown
	}
	ids := make([]OrderID, len(r.orders))
	for i, o := range r.orders {
		ids[i] = o.ID
	}
	slices.Sort(ids)
	return ids[int(sel)%len(ids)]
}

func decodePrice(v byte) int64 {
	if v == 255 {
		return 0 // invalid
	}
	return 95 + int64(v%11) // a narrow band, so orders cross often
}

func decodeOrder(op []byte, id OrderID) Order {
	f := op[1]
	o := Order{ID: id, Side: Buy, Type: Limit, Price: decodePrice(op[2]), Qty: int64(op[3] % 12)}
	if f&1 != 0 {
		o.Side = Sell
	}
	o.TIF = [...]TimeInForce{GTC, IOC, FOK, GTC}[(f>>3)&3]
	if f&2 != 0 && f&4 != 0 {
		o.Type = Market
		if o.TIF == GTC && op[5] != 252 {
			o.TIF = IOC
		}
	}
	o.PostOnly = f&0x20 != 0 && f&0x40 != 0
	o.Owner = uint64(op[4] % 4)
	if v := (op[4] >> 2) & 15; v == 15 {
		o.STP = 3 // invalid
	} else {
		o.STP = STP(v % 3)
	}
	switch op[5] {
	case 255:
		o.Side = 0
	case 254:
		o.Type = 3
	case 253:
		o.TIF = 0
	}
	return o
}

// runScript decodes data into a sequence of commands, runs them on a Book and on the
// reference, and fails at the first difference in events, resting orders, invariants or
// the incremental level changes.
func runScript(t *testing.T, data []byte) {
	const opSize = 6
	if len(data) > 600*opSize {
		data = data[:600*opSize]
	}
	b, r := New(), &refBook{}
	depth := map[levelKey]int64{} // maintained only from b.Changes()
	nextID := OrderID(1)
	for step := 0; len(data) >= opSize; step++ {
		op := data[:opSize]
		data = data[opSize:]
		var got, want []Event
		unknown := nextID + 1_000_000
		switch op[0] % 8 {
		case 5:
			id := r.pick(op[5], unknown)
			got, want = slices.Clone(b.Cancel(id)), r.cancel(id)
		case 6:
			id := r.pick(op[5], unknown)
			price, qty := decodePrice(op[2]), int64(op[3]%12)
			got, want = slices.Clone(b.Modify(id, price, qty)), r.modify(id, price, qty)
		case 7: // reuse a live ID: must be rejected as a duplicate
			o := decodeOrder(op, r.pick(op[5], nextID))
			nextID++
			got, want = slices.Clone(b.Submit(o)), r.submit(o)
		default:
			o := decodeOrder(op, nextID)
			nextID++
			got, want = slices.Clone(b.Submit(o)), r.submit(o)
		}
		if !slices.Equal(got, want) {
			t.Fatalf("step %d (op % x): events differ\n got:%s\nwant:%s", step, op, fmtEvents(got), fmtEvents(want))
		}
		for _, s := range []Side{Buy, Sell} {
			if g, w := slices.Collect(b.Orders(s)), r.sorted(s); !slices.Equal(g, w) {
				t.Fatalf("step %d: %s orders differ\n got: %v\nwant: %v", step, s, g, w)
			}
		}
		if err := invariantError(b); err != nil {
			t.Fatalf("step %d: %v", step, err)
		}
		for _, c := range b.Changes() {
			k := levelKey{c.Side, c.Price}
			if c.Qty == 0 {
				delete(depth, k)
			} else {
				depth[k] = c.Qty
			}
		}
		if want := r.depth(); !maps.Equal(depth, want) {
			t.Fatalf("step %d: depth rebuilt from Changes() = %v, want %v", step, depth, want)
		}
	}
}

func FuzzAgainstReference(f *testing.F) {
	f.Add([]byte{
		0, 0, 5, 3, 0, 0, // buy 3 @100
		1, 1, 5, 2, 0, 0, // sell 2 @100: trades
		2, 1, 7, 5, 1, 0, // sell 5 @102, owner 1
		3, 0, 9, 9, 1, 0, // buy 9 @104, owner 1: self-trade
		6, 0, 3, 4, 0, 0, // modify
		5, 0, 0, 0, 0, 0, // cancel
	})
	f.Add([]byte{0, 0x10, 5, 4, 0, 0, 1, 1, 3, 3, 2, 0, 2, 0x08, 6, 6, 3, 9, 4, 0x66, 4, 2, 0, 0})
	rng := rand.New(rand.NewPCG(1, 2))
	for range 8 {
		seed := make([]byte, 6*64)
		for i := range seed {
			seed[i] = byte(rng.Uint32())
		}
		f.Add(seed)
	}
	f.Fuzz(runScript)
}

// TestRandomAgainstReference runs the same comparison on long pseudo-random scripts, so
// plain `go test` covers it without -fuzz.
func TestRandomAgainstReference(t *testing.T) {
	scripts := 200
	if testing.Short() {
		scripts = 20
	}
	for seed := range uint64(scripts) {
		rng := rand.New(rand.NewPCG(seed, 99))
		data := make([]byte, 6*600)
		for i := range data {
			data[i] = byte(rng.Uint32())
		}
		runScript(t, data)
	}
}
