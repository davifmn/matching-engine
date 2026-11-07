// Package flow generates synthetic order flow that looks like a real venue's: most orders
// rest a few ticks from the spread, a quarter of the messages are cancellations, and a
// minority cross the spread. Benchmarks, the load generator and the recovery tests use it,
// so they all exercise the same mix.
package flow

import (
	"math/rand/v2"

	"github.com/davifmn/matching-engine/internal/book"
)

// Kind of operation.
type Kind uint8

const (
	Passive    Kind = iota // limit order priced away from the spread; usually rests
	Aggressive             // limit order priced through the spread; usually trades
	Market                 // market IOC order
	Cancel                 // cancel a live order
	Modify                 // modify a live order
)

func (k Kind) String() string {
	return [...]string{"passive", "aggressive", "market", "cancel", "modify"}[k]
}

// Mix holds the relative weights of each Kind.
type Mix struct {
	Passive, Aggressive, Market, Cancel, Modify int
}

// DefaultMix is roughly the message mix of a liquid equity on a lit venue.
var DefaultMix = Mix{Passive: 55, Aggressive: 10, Market: 5, Cancel: 25, Modify: 5}

// Op is one generated operation.
type Op struct {
	Kind  Kind
	Side  book.Side // new orders
	Price int64     // limit orders, in ticks
	Qty   int64     // new orders, in lots
	// Cancel and Modify: index into the caller's list of live orders (see Live).
	Pick int
	// Modify: true moves the order one tick away from the spread (a cancel-replace),
	// false halves its quantity (an in-place reduction that keeps queue priority).
	Reprice bool
}

// Generator produces a deterministic sequence of operations for a seed.
type Generator struct {
	rng   *rand.Rand
	mid   int64
	mix   Mix
	total int
}

// New returns a generator whose mid price starts at mid ticks and random-walks.
func New(seed uint64, mid int64, mix Mix) *Generator {
	return &Generator{
		rng:   rand.New(rand.NewPCG(seed, 0x9e3779b97f4a7c15)),
		mid:   max(mid, 100),
		mix:   mix,
		total: mix.Passive + mix.Aggressive + mix.Market + mix.Cancel + mix.Modify,
	}
}

// Mid returns the current mid price in ticks.
func (g *Generator) Mid() int64 { return g.mid }

// Next returns the next operation. live is the number of live orders the caller is
// tracking; with none, cancels and modifies are replaced by passive orders.
func (g *Generator) Next(live int) Op {
	if g.rng.IntN(10) == 0 {
		g.mid = max(g.mid+int64(g.rng.IntN(3))-1, 100)
	}
	k := g.kind()
	if (k == Cancel || k == Modify) && live == 0 {
		k = Passive
	}
	op := Op{Kind: k, Side: book.Buy}
	if g.rng.IntN(2) == 0 {
		op.Side = book.Sell
	}
	switch k {
	case Passive:
		op.Price = g.away(op.Side, 1+g.geometric(0.25))
		op.Qty = g.qty()
	case Aggressive:
		op.Price = g.away(op.Side, -1-g.geometric(0.6))
		op.Qty = g.qty()
	case Market:
		op.Qty = g.qty()
	case Cancel:
		op.Pick = g.rng.IntN(live)
	case Modify:
		op.Pick = g.rng.IntN(live)
		op.Reprice = g.rng.IntN(2) == 0
	}
	return op
}

func (g *Generator) kind() Kind {
	n := g.rng.IntN(g.total)
	for k, w := range [...]int{g.mix.Passive, g.mix.Aggressive, g.mix.Market, g.mix.Cancel, g.mix.Modify} {
		if n < w {
			return Kind(k)
		}
		n -= w
	}
	return Passive
}

// away returns a price d ticks away from the mid on the passive side of s; a negative d
// goes through the mid instead.
func (g *Generator) away(s book.Side, d int64) int64 {
	if s == book.Buy {
		return max(g.mid-d, 1)
	}
	return max(g.mid+d, 1)
}

// geometric returns the number of failures before a success of probability p, capped.
func (g *Generator) geometric(p float64) int64 {
	var n int64
	for n < 50 && g.rng.Float64() > p {
		n++
	}
	return n
}

func (g *Generator) qty() int64 {
	if g.rng.IntN(20) == 0 {
		return 10 + int64(g.rng.IntN(90)) // the occasional large order
	}
	return 1 + int64(g.rng.IntN(10))
}

// Live tracks the IDs of resting orders from book events, with O(1) add, remove and
// pick-by-index, so callers can turn Cancel and Modify ops into real order IDs.
type Live struct {
	ids []book.OrderID
	pos map[book.OrderID]int
}

// NewLive returns an empty set.
func NewLive() *Live { return &Live{pos: make(map[book.OrderID]int)} }

// Len returns the number of live orders.
func (l *Live) Len() int { return len(l.ids) }

// At returns the i-th live order.
func (l *Live) At(i int) book.OrderID { return l.ids[i] }

// Add records a resting order.
func (l *Live) Add(id book.OrderID) {
	if _, ok := l.pos[id]; !ok {
		l.pos[id] = len(l.ids)
		l.ids = append(l.ids, id)
	}
}

// Remove forgets an order; unknown IDs are ignored.
func (l *Live) Remove(id book.OrderID) {
	i, ok := l.pos[id]
	if !ok {
		return
	}
	last := l.ids[len(l.ids)-1]
	l.ids[i] = last
	l.pos[last] = i
	l.ids = l.ids[:len(l.ids)-1]
	delete(l.pos, id)
}

// Apply updates the set from the events of one book operation.
func (l *Live) Apply(events []book.Event) {
	for _, e := range events {
		switch e.Kind {
		case book.EventRested:
			l.Add(e.Order)
		case book.EventCanceled:
			l.Remove(e.Order)
		case book.EventTrade:
			if e.MakerLeaves == 0 {
				l.Remove(e.Maker)
			}
			if e.Leaves == 0 {
				l.Remove(e.Order) // a modified order that traded away completely
			}
		}
	}
}
