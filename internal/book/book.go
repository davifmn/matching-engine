package book

import (
	"cmp"
	"encoding/binary"
	"hash/fnv"
	"iter"
	"slices"
)

// order is a resting order: a node in the intrusive FIFO list of its price level, so a
// cancel unlinks it in O(1) without searching the queue.
type order struct {
	id         OrderID
	owner      uint64
	price      int64
	qty        int64
	side       Side
	postOnly   bool
	stp        STP
	lvl        *level
	prev, next *order
}

func (m *order) info() OrderInfo {
	return OrderInfo{ID: m.id, Owner: m.owner, Side: m.side, Price: m.price, Qty: m.qty, PostOnly: m.postOnly, STP: m.stp}
}

// level is the FIFO queue of orders at one price.
type level struct {
	price      int64
	qty        int64 // sum of the open quantity of its orders
	n          int   // number of orders
	head, tail *order
	epoch      uint64 // last operation that recorded a change to this level
}

// bookSide keeps the price levels of one side in a slice sorted from the worst price to the
// best, so the best level is the last element: reading it is O(1), removing it when it
// empties is O(1), and new levels near the top of the book shift only a few pointers.
// docs/adr/0002-price-levels.md compares this with a tree.
type bookSide struct {
	side   Side
	levels []*level
}

func (s *bookSide) best() *level {
	if len(s.levels) == 0 {
		return nil
	}
	return s.levels[len(s.levels)-1]
}

// rank maps a price so that a higher rank is a better price for this side.
func (s *bookSide) rank(p int64) int64 {
	if s.side == Buy {
		return p
	}
	return -p
}

// search returns the index of the level at price p, or the index where it would go.
func (s *bookSide) search(p int64) (int, bool) {
	r := s.rank(p)
	lo, hi := 0, len(s.levels)
	if hi > 0 { // fast path: most activity is at the top of the book
		top := s.rank(s.levels[hi-1].price)
		if r == top {
			return hi - 1, true
		}
		if r > top {
			return hi, false
		}
	}
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if s.rank(s.levels[mid].price) < r {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo, lo < len(s.levels) && s.levels[lo].price == p
}

// Book is a limit order book for one instrument. The zero value is not usable; call New.
type Book struct {
	bids, asks bookSide
	orders     map[OrderID]*order

	events  []Event       // events of the current operation, reused between calls
	touched []LevelChange // levels changed by the current operation (Qty filled in by Changes)
	changes []LevelChange
	epoch   uint64

	// Free lists: in steady state the book allocates nothing, so the garbage collector
	// has no work caused by the matching path.
	freeOrders []*order
	freeLevels []*level
}

// New returns an empty book.
func New() *Book {
	return &Book{
		bids:   bookSide{side: Buy},
		asks:   bookSide{side: Sell},
		orders: make(map[OrderID]*order),
	}
}

func (b *Book) sideOf(s Side) *bookSide {
	if s == Buy {
		return &b.bids
	}
	return &b.asks
}

// Submit matches a new order against the book. What is left of a GTC limit order rests on
// the book; what is left of an IOC or market order is canceled. The returned slice is
// reused: it is valid until the next call on the Book.
func (b *Book) Submit(o Order) []Event {
	b.begin()
	if r := validate(&o); r != ReasonNone {
		return b.reject(o.ID, o.Side, o.Price, o.Qty, r)
	}
	if _, dup := b.orders[o.ID]; dup {
		return b.reject(o.ID, o.Side, o.Price, o.Qty, ReasonDuplicateID)
	}
	if o.Type == Market {
		o.Price = 0
	}
	if o.PostOnly && b.crosses(o.Side, o.Price) {
		return b.reject(o.ID, o.Side, o.Price, o.Qty, ReasonPostOnlyWouldCross)
	}
	if o.TIF == FOK && !b.fillable(&o) {
		return b.reject(o.ID, o.Side, o.Price, o.Qty, ReasonFOKNotFilled)
	}
	b.emit(Event{Kind: EventAccepted, Order: o.ID, Side: o.Side, Price: o.Price, Qty: o.Qty, Leaves: o.Qty})
	b.finish(&o, b.match(&o))
	return b.events
}

// Cancel removes a resting order.
func (b *Book) Cancel(id OrderID) []Event {
	b.begin()
	m, ok := b.orders[id]
	if !ok {
		return b.reject(id, 0, 0, 0, ReasonUnknownOrder)
	}
	b.cancelResting(m, ReasonCanceledByUser)
	return b.events
}

// Modify changes the price and the open quantity of a resting order.
//
// Lowering the quantity at the same price keeps the order's place in the queue. Any other
// change is a cancel-replace: the order goes to the back of the queue at the new price and
// trades at once if the new price crosses the spread (a post-only order is rejected
// instead, and stays as it was).
func (b *Book) Modify(id OrderID, price, qty int64) []Event {
	b.begin()
	m, ok := b.orders[id]
	if !ok {
		return b.reject(id, 0, price, qty, ReasonUnknownOrder)
	}
	side := m.side
	switch {
	case qty <= 0:
		return b.reject(id, side, price, qty, ReasonInvalidQty)
	case price <= 0:
		return b.reject(id, side, price, qty, ReasonInvalidPrice)
	}
	if price == m.price && qty <= m.qty {
		if d := m.qty - qty; d > 0 {
			m.qty = qty
			m.lvl.qty -= d
			b.touch(side, m.lvl)
		}
		b.emit(Event{Kind: EventModified, Order: id, Side: side, Price: price, Qty: qty, Leaves: qty})
		return b.events
	}
	if m.postOnly && b.crosses(side, price) {
		return b.reject(id, side, price, qty, ReasonPostOnlyWouldCross)
	}
	o := Order{ID: id, Owner: m.owner, Side: side, Type: Limit, TIF: GTC, Price: price, Qty: qty, PostOnly: m.postOnly, STP: m.stp}
	b.remove(m)
	b.emit(Event{Kind: EventModified, Order: id, Side: side, Price: price, Qty: qty, Leaves: qty})
	b.finish(&o, b.match(&o))
	return b.events
}

func (b *Book) begin() {
	b.events = b.events[:0]
	b.touched = b.touched[:0]
	b.epoch++
}

func (b *Book) emit(e Event) { b.events = append(b.events, e) }

func (b *Book) reject(id OrderID, s Side, price, qty int64, r Reason) []Event {
	b.emit(Event{Kind: EventRejected, Reason: r, Order: id, Side: s, Price: price, Qty: qty})
	return b.events
}

func (b *Book) touch(s Side, l *level) {
	if l.epoch != b.epoch {
		l.epoch = b.epoch
		b.touched = append(b.touched, LevelChange{Side: s, Price: l.price})
	}
}

// crosses reports whether a limit order at price would trade at once.
func (b *Book) crosses(s Side, price int64) bool {
	lvl := b.sideOf(s.Opposite()).best()
	return lvl != nil && acceptable(s, price, lvl.price)
}

// fillable reports whether matching o now would fill it completely. It walks the queue in
// the same order as match, so a resting order of the same owner stops the walk exactly
// where self-trade prevention would stop the match.
func (b *Book) fillable(o *Order) bool {
	need := o.Qty
	opp := b.sideOf(o.Side.Opposite())
	for i := len(opp.levels) - 1; i >= 0; i-- {
		lvl := opp.levels[i]
		if o.Type == Limit && !acceptable(o.Side, o.Price, lvl.price) {
			return false
		}
		if o.Owner == 0 { // no STP: the level total is enough
			if lvl.qty >= need {
				return true
			}
			need -= lvl.qty
			continue
		}
		for m := lvl.head; m != nil; m = m.next {
			if m.owner == o.Owner {
				if o.STP == CancelOldest {
					continue // it would be canceled, not traded with
				}
				return false
			}
			if m.qty >= need {
				return true
			}
			need -= m.qty
		}
	}
	return false
}

// match trades o against the opposite side, best price first and oldest order first within
// a price, until o is filled, no acceptable price is left, or self-trade prevention stops
// it. It reports whether self-trade prevention stopped it.
func (b *Book) match(o *Order) (stopped bool) {
	opp := b.sideOf(o.Side.Opposite())
	for o.Qty > 0 {
		lvl := opp.best()
		if lvl == nil || (o.Type == Limit && !acceptable(o.Side, o.Price, lvl.price)) {
			return false
		}
		for m := lvl.head; m != nil && o.Qty > 0; {
			next := m.next
			if o.Owner != 0 && m.owner == o.Owner {
				switch o.STP {
				case CancelNewest:
					return true
				case CancelOldest:
					b.cancelResting(m, ReasonSelfTrade)
					m = next
					continue
				default: // CancelBoth
					b.cancelResting(m, ReasonSelfTrade)
					return true
				}
			}
			q := min(o.Qty, m.qty)
			o.Qty -= q
			m.qty -= q
			lvl.qty -= q
			b.touch(opp.side, lvl)
			b.emit(Event{
				Kind: EventTrade, Order: o.ID, Side: o.Side, Price: lvl.price, Qty: q,
				Leaves: o.Qty, Maker: m.id, MakerLeaves: m.qty,
			})
			if m.qty == 0 {
				b.remove(m) // frees the level too when m was its last order
			}
			m = next
		}
	}
	return false
}

// finish decides what happens to the part of o that did not trade.
func (b *Book) finish(o *Order, stopped bool) {
	if o.Qty == 0 {
		return
	}
	switch {
	case stopped:
		b.emit(Event{Kind: EventCanceled, Reason: ReasonSelfTrade, Order: o.ID, Side: o.Side, Price: o.Price, Qty: o.Qty})
	case o.Type == Limit && o.TIF == GTC:
		b.rest(o)
		b.emit(Event{Kind: EventRested, Order: o.ID, Side: o.Side, Price: o.Price, Qty: o.Qty, Leaves: o.Qty})
	default:
		b.emit(Event{Kind: EventCanceled, Reason: ReasonNoLiquidity, Order: o.ID, Side: o.Side, Price: o.Price, Qty: o.Qty})
	}
}

func (b *Book) rest(o *Order) {
	s := b.sideOf(o.Side)
	i, found := s.search(o.Price)
	var lvl *level
	if found {
		lvl = s.levels[i]
	} else {
		lvl = b.newLevel()
		lvl.price = o.Price
		s.levels = slices.Insert(s.levels, i, lvl)
	}
	m := b.newOrder()
	*m = order{
		id: o.ID, owner: o.Owner, price: o.Price, qty: o.Qty, side: o.Side,
		postOnly: o.PostOnly, stp: o.STP, lvl: lvl, prev: lvl.tail,
	}
	if lvl.tail != nil {
		lvl.tail.next = m
	} else {
		lvl.head = m
	}
	lvl.tail = m
	lvl.qty += m.qty
	lvl.n++
	b.touch(o.Side, lvl)
	b.orders[o.ID] = m
}

func (b *Book) cancelResting(m *order, r Reason) {
	b.emit(Event{Kind: EventCanceled, Reason: r, Order: m.id, Side: m.side, Price: m.price, Qty: m.qty})
	b.remove(m)
}

// remove unlinks a resting order, and its level if the level becomes empty.
func (b *Book) remove(m *order) {
	lvl := m.lvl
	if m.prev != nil {
		m.prev.next = m.next
	} else {
		lvl.head = m.next
	}
	if m.next != nil {
		m.next.prev = m.prev
	} else {
		lvl.tail = m.prev
	}
	lvl.qty -= m.qty
	lvl.n--
	b.touch(m.side, lvl)
	delete(b.orders, m.id)
	if lvl.n == 0 {
		s := b.sideOf(m.side)
		i, _ := s.search(lvl.price)
		s.levels = slices.Delete(s.levels, i, i+1)
		*lvl = level{}
		b.freeLevels = append(b.freeLevels, lvl)
	}
	*m = order{}
	b.freeOrders = append(b.freeOrders, m)
}

func (b *Book) newOrder() *order {
	if n := len(b.freeOrders); n > 0 {
		m := b.freeOrders[n-1]
		b.freeOrders = b.freeOrders[:n-1]
		return m
	}
	return new(order)
}

func (b *Book) newLevel() *level {
	if n := len(b.freeLevels); n > 0 {
		l := b.freeLevels[n-1]
		b.freeLevels = b.freeLevels[:n-1]
		return l
	}
	return new(level)
}

// Changes returns the levels changed by the last Submit, Cancel or Modify, sorted by side
// and price, with their new total quantity. The slice is valid until the next call.
func (b *Book) Changes() []LevelChange {
	slices.SortFunc(b.touched, func(x, y LevelChange) int {
		if c := cmp.Compare(x.Side, y.Side); c != 0 {
			return c
		}
		return cmp.Compare(x.Price, y.Price)
	})
	b.touched = slices.Compact(b.touched)
	b.changes = b.changes[:0]
	for _, t := range b.touched {
		s := b.sideOf(t.Side)
		if i, ok := s.search(t.Price); ok {
			t.Qty = s.levels[i].qty
		}
		b.changes = append(b.changes, t)
	}
	return b.changes
}

// Len returns the number of resting orders.
func (b *Book) Len() int { return len(b.orders) }

// Levels returns the number of price levels on one side.
func (b *Book) Levels(s Side) int { return len(b.sideOf(s).levels) }

// Best returns the best level of one side.
func (b *Book) Best(s Side) (PriceLevel, bool) {
	lvl := b.sideOf(s).best()
	if lvl == nil {
		return PriceLevel{}, false
	}
	return PriceLevel{Price: lvl.price, Qty: lvl.qty, Orders: lvl.n}, true
}

// Depth appends up to n levels of one side to dst, best first. n <= 0 means all levels.
func (b *Book) Depth(s Side, n int, dst []PriceLevel) []PriceLevel {
	levels := b.sideOf(s).levels
	if n <= 0 || n > len(levels) {
		n = len(levels)
	}
	for i := len(levels) - 1; i >= len(levels)-n; i-- {
		l := levels[i]
		dst = append(dst, PriceLevel{Price: l.price, Qty: l.qty, Orders: l.n})
	}
	return dst
}

// Lookup returns a resting order.
func (b *Book) Lookup(id OrderID) (OrderInfo, bool) {
	m, ok := b.orders[id]
	if !ok {
		return OrderInfo{}, false
	}
	return m.info(), true
}

// Orders iterates over the resting orders of one side in priority order: best price
// first, and oldest first within a price.
func (b *Book) Orders(s Side) iter.Seq[OrderInfo] {
	return func(yield func(OrderInfo) bool) {
		levels := b.sideOf(s).levels
		for i := len(levels) - 1; i >= 0; i-- {
			for m := levels[i].head; m != nil; m = m.next {
				if !yield(m.info()) {
					return
				}
			}
		}
	}
}

// Digest is a hash of the whole book state, queue positions included. Two books that got
// the same commands have the same digest; recovery tests and replicas compare it.
func (b *Book) Digest() uint64 {
	h := fnv.New64a()
	var buf [8]byte
	put := func(v uint64) {
		binary.LittleEndian.PutUint64(buf[:], v)
		h.Write(buf[:])
	}
	for _, s := range []Side{Buy, Sell} {
		put(uint64(s))
		for o := range b.Orders(s) {
			flags := uint64(o.STP) << 1
			if o.PostOnly {
				flags |= 1
			}
			put(uint64(o.ID))
			put(o.Owner)
			put(uint64(o.Price))
			put(uint64(o.Qty))
			put(flags)
		}
	}
	return h.Sum64()
}
