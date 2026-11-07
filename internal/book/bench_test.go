package book_test

import (
	"sync"
	"testing"
	"time"

	"github.com/davifmn/matching-engine/internal/book"
	"github.com/davifmn/matching-engine/internal/flow"
	"github.com/davifmn/matching-engine/internal/latency"
)

// command is one recorded book call, so benchmarks replay identical work on every run.
type command struct {
	kind  flow.Kind
	order book.Order
	id    book.OrderID
	price int64
	qty   int64
}

func (c command) apply(b *book.Book) []book.Event {
	switch c.kind {
	case flow.Cancel:
		return b.Cancel(c.id)
	case flow.Modify:
		return b.Modify(c.id, c.price, c.qty)
	default:
		return b.Submit(c.order)
	}
}

// record turns generated flow into concrete book commands by running it once.
func record(n int, seed uint64) []command {
	g := flow.New(seed, 10_000, flow.DefaultMix)
	b, live := book.New(), flow.NewLive()
	cmds := make([]command, 0, n)
	next := book.OrderID(1)
	for range n {
		op := g.Next(live.Len())
		c := command{kind: op.Kind}
		switch op.Kind {
		case flow.Passive, flow.Aggressive:
			c.order = book.Order{ID: next, Owner: uint64(next % 50), Side: op.Side, Type: book.Limit, TIF: book.GTC, Price: op.Price, Qty: op.Qty}
			next++
		case flow.Market:
			c.order = book.Order{ID: next, Owner: uint64(next % 50), Side: op.Side, Type: book.Market, TIF: book.IOC, Qty: op.Qty}
			next++
		case flow.Cancel:
			c.id = live.At(op.Pick)
		case flow.Modify:
			c.id = live.At(op.Pick)
			info, _ := b.Lookup(c.id)
			c.price, c.qty = info.Price, max(info.Qty/2, 1)
			if op.Reprice {
				c.qty = info.Qty
				if info.Side == book.Buy {
					c.price--
				} else {
					c.price++
				}
			}
		}
		live.Apply(c.apply(b))
		cmds = append(cmds, c)
	}
	return cmds
}

var recorded = sync.OnceValue(func() []command { return record(500_000, 42) })

// BenchmarkRealisticFlow replays half a million recorded commands of the default mix
// (55% passive, 10% aggressive, 5% market, 25% cancel, 5% modify). ns/op is per command.
func BenchmarkRealisticFlow(b *testing.B) {
	cmds := recorded()
	bk := book.New()
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		j := i % len(cmds)
		if j == 0 && i > 0 {
			b.StopTimer()
			bk = book.New()
			b.StartTimer()
		}
		cmds[j].apply(bk)
		bk.Changes()
	}
}

// BenchmarkRealisticFlowLatency is the same replay, timing every command to report the
// latency distribution. Timing adds roughly the cost of two clock reads per command.
func BenchmarkRealisticFlowLatency(b *testing.B) {
	cmds := recorded()
	bk := book.New()
	var h latency.Histogram
	b.ResetTimer()
	for i := range b.N {
		j := i % len(cmds)
		if j == 0 && i > 0 {
			bk = book.New()
		}
		start := time.Now()
		cmds[j].apply(bk)
		bk.Changes()
		h.Record(time.Since(start))
	}
	b.ReportMetric(float64(h.Quantile(0.50).Nanoseconds()), "p50-ns")
	b.ReportMetric(float64(h.Quantile(0.99).Nanoseconds()), "p99-ns")
	b.ReportMetric(float64(h.Quantile(0.999).Nanoseconds()), "p99.9-ns")
}

// populated returns a book with depth levels of resting orders on each side.
func populated(depth int) *book.Book {
	b := book.New()
	id := book.OrderID(1)
	for i := range int64(depth) {
		for range 4 {
			b.Submit(book.Order{ID: id, Side: book.Buy, Type: book.Limit, TIF: book.GTC, Price: 9_999 - i, Qty: 10})
			b.Submit(book.Order{ID: id + 1, Side: book.Sell, Type: book.Limit, TIF: book.GTC, Price: 10_001 + i, Qty: 10})
			id += 2
		}
	}
	return b
}

// BenchmarkRestAndCancel adds a passive order inside a 1,000-level book and cancels it.
func BenchmarkRestAndCancel(b *testing.B) {
	bk := populated(1000)
	id := book.OrderID(1 << 40)
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		id++
		price := 9_999 - int64(i%64)
		bk.Submit(book.Order{ID: id, Side: book.Buy, Type: book.Limit, TIF: book.GTC, Price: price, Qty: 1})
		bk.Cancel(id)
	}
}

// BenchmarkTakeTopOfBook is one aggressive IOC order trading with the best resting order.
func BenchmarkTakeTopOfBook(b *testing.B) {
	bk := populated(100)
	bk.Submit(book.Order{ID: 1 << 50, Side: book.Sell, Type: book.Limit, TIF: book.GTC, Price: 10_000, Qty: 1 << 50})
	id := book.OrderID(1 << 40)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		id++
		bk.Submit(book.Order{ID: id, Side: book.Buy, Type: book.Limit, TIF: book.IOC, Price: 10_000, Qty: 1})
	}
}

// BenchmarkSweepTenLevels is a market order that empties the ten best ask levels (40
// orders), including the cost of putting them back.
func BenchmarkSweepTenLevels(b *testing.B) {
	bk := book.New()
	id := book.OrderID(1)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		for p := range int64(10) {
			for range 4 {
				bk.Submit(book.Order{ID: id, Side: book.Sell, Type: book.Limit, TIF: book.GTC, Price: 10_001 + p, Qty: 5})
				id++
			}
		}
		bk.Submit(book.Order{ID: id, Side: book.Buy, Type: book.Market, TIF: book.IOC, Qty: 200})
		id++
	}
}
