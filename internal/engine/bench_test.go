package engine

import (
	"sync/atomic"
	"testing"

	"github.com/davifmn/matching-engine/internal/book"
	"github.com/davifmn/matching-engine/internal/journal"
)

// BenchmarkEngine measures one command through the whole engine (queue, journal, matching,
// reply) with many concurrent clients. Each client alternates submitting a passive order
// and cancelling it, so the book stays small. With fsync, group commit spreads one fsync
// over every command that arrived while the previous one ran.
func BenchmarkEngine(b *testing.B) {
	for _, bc := range []struct {
		name    string
		journal bool
		sync    journal.SyncMode
	}{
		{"memory", false, journal.SyncNever},
		{"journal-nosync", true, journal.SyncNever},
		{"journal-fsync", true, journal.SyncEveryCommit},
	} {
		b.Run(bc.name, func(b *testing.B) {
			dir := ""
			if bc.journal {
				dir = b.TempDir()
			}
			e, err := New(Config{Dir: dir, Sync: bc.sync}, "BENCH")
			if err != nil {
				b.Fatal(err)
			}
			defer e.Close()
			var client atomic.Int64
			b.SetParallelism(16)
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				c := client.Add(1)
				side := book.Side(1 + c%2)
				price := int64(10_000 - 10)
				if side == book.Sell {
					price = 10_000 + 10
				}
				var last book.OrderID
				for pb.Next() {
					if last == 0 {
						r, err := e.Submit(ctx, "BENCH", book.Order{Owner: uint64(c), Side: side, Type: book.Limit, TIF: book.GTC, Price: price, Qty: 1})
						if err != nil {
							b.Error(err)
							return
						}
						last = r.OrderID
					} else {
						if _, err := e.Cancel(ctx, "BENCH", last); err != nil {
							b.Error(err)
							return
						}
						last = 0
					}
				}
			})
		})
	}
}
