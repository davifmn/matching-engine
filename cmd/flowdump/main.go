// Command flowdump writes a deterministic flow of book commands together with what this
// book does with them, so that another implementation can be checked against this one,
// command by command (orderbook-rs, the Rust port, does).
//
// The flow is internal/flow's realistic mix, with accounts, self-trade prevention modes,
// post-only and fill-or-kill orders added so that every rule is exercised.
//
// File format, little endian:
//
//	"OBFLOW01" | count u32 | checkpoint interval u32
//	count commands, each a kind byte then:
//	  1 submit: id u64, owner u64, side u8, type u8, tif u8, stp u8, post-only u8, price i64, qty i64
//	  2 cancel: id u64
//	  3 modify: id u64, price i64, qty i64
//	events u64 | trades u64 | book digest u64 | event hash u64 | checkpoints u32 | hash u64 × checkpoints
//
// The event hash is FNV-1a over every event's fields as u64 (kind, reason, order, side,
// price, qty, leaves, maker, maker leaves, owner, maker owner); a checkpoint is its value
// after every interval commands, to locate the first difference.
package main

import (
	"bufio"
	"encoding/binary"
	"flag"
	"fmt"
	"hash"
	"hash/fnv"
	"os"

	"github.com/davifmn/matching-engine/internal/book"
	"github.com/davifmn/matching-engine/internal/flow"
)

func main() {
	n := flag.Int("n", 50_000, "number of commands")
	seed := flag.Uint64("seed", 42, "flow seed")
	every := flag.Int("checkpoint", 1000, "commands between checkpoints")
	out := flag.String("o", "flow.bin", "output file")
	flag.Parse()
	if err := dump(*out, *n, *seed, *every); err != nil {
		fmt.Fprintln(os.Stderr, "flowdump:", err)
		os.Exit(1)
	}
}

type writer struct {
	w   *bufio.Writer
	buf [8]byte
}

func (w *writer) u8(v uint8) { w.w.WriteByte(v) }
func (w *writer) u32(v uint32) {
	binary.LittleEndian.PutUint32(w.buf[:4], v)
	w.w.Write(w.buf[:4])
}
func (w *writer) u64(v uint64) {
	binary.LittleEndian.PutUint64(w.buf[:], v)
	w.w.Write(w.buf[:])
}

func hashEvent(h hash.Hash64, e book.Event) {
	var b [8]byte
	for _, v := range []uint64{uint64(e.Kind), uint64(e.Reason), uint64(e.Order), uint64(e.Side),
		uint64(e.Price), uint64(e.Qty), uint64(e.Leaves), uint64(e.Maker), uint64(e.MakerLeaves),
		e.Owner, e.MakerOwner} {
		binary.LittleEndian.PutUint64(b[:], v)
		h.Write(b[:])
	}
}

func b2u(b bool) uint8 {
	if b {
		return 1
	}
	return 0
}

func dump(path string, n int, seed uint64, every int) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := &writer{w: bufio.NewWriter(f)}
	w.w.WriteString("OBFLOW01")
	w.u32(uint32(n))
	w.u32(uint32(every))

	g := flow.New(seed, 10_000, flow.DefaultMix)
	live := flow.NewLive()
	bk := book.New()
	h := fnv.New64a()
	var nextID book.OrderID
	var events, trades uint64
	var checkpoints []uint64
	reasons := map[book.Reason]int{}

	for i := range n {
		op := g.Next(live.Len())
		var ev []book.Event
		switch op.Kind {
		case flow.Passive, flow.Aggressive, flow.Market:
			nextID++
			o := book.Order{ID: nextID, Owner: 1 + uint64(i%7), Side: op.Side, Type: book.Limit, TIF: book.GTC,
				Price: op.Price, Qty: op.Qty, STP: book.STP(i % 3)}
			switch {
			case op.Kind == flow.Market:
				o.Type, o.Price, o.TIF = book.Market, 0, book.IOC
				if i%5 == 0 {
					o.TIF = book.FOK
				}
			case op.Kind == flow.Passive && i%13 == 0:
				o.PostOnly = true
			case op.Kind == flow.Aggressive && i%11 == 0:
				o.TIF = book.FOK
			case op.Kind == flow.Aggressive && i%4 == 0:
				o.TIF = book.IOC
			}
			w.u8(1)
			w.u64(uint64(o.ID))
			w.u64(o.Owner)
			w.u8(uint8(o.Side))
			w.u8(uint8(o.Type))
			w.u8(uint8(o.TIF))
			w.u8(uint8(o.STP))
			w.u8(b2u(o.PostOnly))
			w.u64(uint64(o.Price))
			w.u64(uint64(o.Qty))
			ev = bk.Submit(o)
		case flow.Cancel:
			id := live.At(op.Pick)
			w.u8(2)
			w.u64(uint64(id))
			ev = bk.Cancel(id)
		case flow.Modify:
			id := live.At(op.Pick)
			info, _ := bk.Lookup(id)
			price, qty := info.Price, max(info.Qty/2, 1)
			if op.Reprice { // one tick away from the spread, same quantity
				qty = info.Qty
				if info.Side == book.Buy {
					price = max(price-1, 1)
				} else {
					price++
				}
			}
			w.u8(3)
			w.u64(uint64(id))
			w.u64(uint64(price))
			w.u64(uint64(qty))
			ev = bk.Modify(id, price, qty)
		}
		for _, e := range ev {
			hashEvent(h, e)
			events++
			if e.Kind == book.EventTrade {
				trades++
			}
			if e.Reason != book.ReasonNone {
				reasons[e.Reason]++
			}
		}
		live.Apply(ev)
		if (i+1)%every == 0 {
			checkpoints = append(checkpoints, h.Sum64())
		}
	}
	w.u64(events)
	w.u64(trades)
	w.u64(bk.Digest())
	w.u64(h.Sum64())
	w.u32(uint32(len(checkpoints)))
	for _, c := range checkpoints {
		w.u64(c)
	}
	if err := w.w.Flush(); err != nil {
		return err
	}
	fmt.Printf("%d commands, %d events, %d trades, %d resting orders, digest %016x, event hash %016x\n",
		n, events, trades, bk.Len(), bk.Digest(), h.Sum64())
	for r := book.ReasonNone + 1; r <= book.ReasonCanceledByUser; r++ {
		if reasons[r] > 0 {
			fmt.Printf("  %-26s %d\n", r, reasons[r])
		}
	}
	return f.Close()
}
