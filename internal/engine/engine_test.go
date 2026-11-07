package engine

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"

	"github.com/davifmn/matching-engine/internal/book"
	"github.com/davifmn/matching-engine/internal/flow"
	"github.com/davifmn/matching-engine/internal/journal"
)

var ctx = context.Background()

func open(t testing.TB, dir string, symbols ...string) *Engine {
	t.Helper()
	e, err := New(Config{Dir: dir, Sync: journal.SyncNever}, symbols...)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func limit(s book.Side, price, qty int64) book.Order {
	return book.Order{Side: s, Type: book.Limit, TIF: book.GTC, Price: price, Qty: qty}
}

// drive sends n generated commands to the engine, tracking live orders so cancels and
// modifies hit real ones.
func drive(t testing.TB, e *Engine, symbol string, seed uint64, n int) {
	t.Helper()
	g := flow.New(seed, 10_000, flow.DefaultMix)
	live := flow.NewLive()
	for range n {
		op := g.Next(live.Len())
		var res Result
		var err error
		switch op.Kind {
		case flow.Passive, flow.Aggressive:
			o := limit(op.Side, op.Price, op.Qty)
			o.Owner = seed%3 + uint64(op.Price%4)
			res, err = e.Submit(ctx, symbol, o)
		case flow.Market:
			res, err = e.Submit(ctx, symbol, book.Order{Side: op.Side, Type: book.Market, TIF: book.IOC, Qty: op.Qty})
		case flow.Cancel:
			res, err = e.Cancel(ctx, symbol, live.At(op.Pick))
		case flow.Modify:
			res, err = e.Modify(ctx, symbol, live.At(op.Pick), g.Mid(), op.Qty+1)
		}
		if err != nil {
			t.Error(err) // not Fatal: drive also runs on other goroutines
			return
		}
		live.Apply(res.Events)
	}
}

func TestSubmitAssignsIDsAndSequence(t *testing.T) {
	e := open(t, "", "PETR4")
	defer e.Close()
	r1, err := e.Submit(ctx, "PETR4", limit(book.Sell, 3415, 2))
	if err != nil {
		t.Fatal(err)
	}
	r2, _ := e.Submit(ctx, "PETR4", limit(book.Buy, 3415, 3))
	if r1.OrderID != 1 || r2.OrderID != 2 || r1.Seq != 1 || r2.Seq != 2 {
		t.Fatalf("ids %d,%d seqs %d,%d", r1.OrderID, r2.OrderID, r1.Seq, r2.Seq)
	}
	kinds := func(ev []book.Event) (k []book.EventKind) {
		for _, e := range ev {
			k = append(k, e.Kind)
		}
		return k
	}
	want := []book.EventKind{book.EventAccepted, book.EventTrade, book.EventRested}
	if got := kinds(r2.Events); !slices.Equal(got, want) {
		t.Errorf("events %v, want %v", got, want)
	}
	c, _ := e.Cancel(ctx, "PETR4", 2)
	if c.OrderID != 2 || c.Events[0].Kind != book.EventCanceled || c.Events[0].Qty != 1 {
		t.Errorf("cancel: %+v", c)
	}
	snap, _ := e.Snapshot(ctx, "PETR4", 10)
	if snap.Seq != 3 || snap.Orders != 0 {
		t.Errorf("snapshot %+v", snap)
	}
}

func TestUnknownInstrument(t *testing.T) {
	e := open(t, "", "PETR4")
	defer e.Close()
	if _, err := e.Submit(ctx, "VALE3", limit(book.Buy, 1, 1)); !errors.Is(err, ErrUnknownInstrument) {
		t.Fatalf("err = %v", err)
	}
	if _, err := New(Config{}, "A", "A"); err == nil {
		t.Fatal("duplicate instrument accepted")
	}
}

// Replaying the journal must rebuild exactly the same book, queue positions included, and
// carry on with the same IDs and sequence numbers.
func TestRestartRebuildsTheSameBook(t *testing.T) {
	dir := t.TempDir()
	e := open(t, dir, "PETR4", "VALE3")
	drive(t, e, "PETR4", 1, 20_000)
	drive(t, e, "VALE3", 2, 5_000)
	last, _ := e.Submit(ctx, "VALE3", limit(book.Buy, 1, 1))
	before := map[string]Snapshot{}
	for _, sym := range e.Symbols() {
		before[sym], _ = e.Digest(ctx, sym)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Submit(ctx, "PETR4", limit(book.Buy, 1, 1)); !errors.Is(err, ErrStopped) {
		t.Fatalf("submit after close: %v", err)
	}

	e2 := open(t, dir, "PETR4", "VALE3")
	defer e2.Close()
	for sym, want := range before {
		got, _ := e2.Digest(ctx, sym)
		if got.Digest != want.Digest || got.Seq != want.Seq || got.Orders != want.Orders {
			t.Errorf("%s after restart: seq %d orders %d digest %x; before: seq %d orders %d digest %x",
				sym, got.Seq, got.Orders, got.Digest, want.Seq, want.Orders, want.Digest)
		}
	}
	if want := before["PETR4"]; want.Orders == 0 {
		t.Fatal("test flow left an empty book; it proves nothing")
	}
	r, _ := e2.Submit(ctx, "VALE3", limit(book.Buy, 1, 1))
	if r.Seq != before["VALE3"].Seq+1 {
		t.Errorf("sequence after restart = %d, want %d", r.Seq, before["VALE3"].Seq+1)
	}
	if r.OrderID != last.OrderID+1 {
		t.Errorf("order ID after restart = %d, want %d", r.OrderID, last.OrderID+1)
	}
}

func TestRestartAfterATornWrite(t *testing.T) {
	dir := t.TempDir()
	e := open(t, dir, "PETR4")
	drive(t, e, "PETR4", 3, 2_000)
	want, _ := e.Digest(ctx, "PETR4")
	e.Close()

	// A crash in the middle of the next write leaves half a record behind.
	path := filepath.Join(dir, "PETR4.wal")
	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	f.Write([]byte{48, 0, 0, 0, 1, 2, 3, 4, 1, 1})
	f.Close()

	e2 := open(t, dir, "PETR4")
	defer e2.Close()
	if got, _ := e2.Digest(ctx, "PETR4"); got.Digest != want.Digest || got.Seq != want.Seq {
		t.Fatalf("after torn write: seq %d digest %x, want seq %d digest %x", got.Seq, got.Digest, want.Seq, want.Digest)
	}
}

func TestCorruptJournalRefusesToStart(t *testing.T) {
	dir := t.TempDir()
	e := open(t, dir, "PETR4")
	drive(t, e, "PETR4", 4, 100)
	e.Close()
	path := filepath.Join(dir, "PETR4.wal")
	raw, _ := os.ReadFile(path)
	raw[20] ^= 0xff // inside the first record
	os.WriteFile(path, raw, 0o600)
	if _, err := New(Config{Dir: dir}, "PETR4"); !errors.Is(err, journal.ErrCorrupt) {
		t.Fatalf("err = %v, want ErrCorrupt", err)
	}
}

func TestJournalFailureStopsTheInstrument(t *testing.T) {
	e := open(t, t.TempDir(), "PETR4", "VALE3")
	defer e.Close()
	// The disk goes away. Setting the field before the next request is safe: sending on
	// the queue orders this write before the shard goroutine reads it.
	e.shards["PETR4"].wal = brokenWAL{}
	if _, err := e.Submit(ctx, "PETR4", limit(book.Buy, 1, 1)); !errors.Is(err, ErrStopped) {
		t.Fatalf("submit with a broken journal: %v", err)
	}
	if _, err := e.Submit(ctx, "PETR4", limit(book.Buy, 1, 1)); !errors.Is(err, ErrStopped) {
		t.Fatalf("instrument kept accepting orders: %v", err)
	}
	if err := e.Ready(); err == nil {
		t.Fatal("Ready() = nil with a stopped instrument")
	}
	if _, err := e.Submit(ctx, "VALE3", limit(book.Buy, 1, 1)); err != nil {
		t.Fatalf("other instruments must keep working: %v", err)
	}
}

func TestConcurrentClients(t *testing.T) {
	e := open(t, t.TempDir(), "PETR4")
	defer e.Close()
	const clients, perClient = 8, 500
	ids := make(chan book.OrderID, clients*perClient)
	var wg sync.WaitGroup
	for c := range clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range perClient {
				o := limit(book.Side(1+(c+i)%2), 100+int64(i%7), 1)
				o.Owner = uint64(c + 1)
				r, err := e.Submit(ctx, "PETR4", o)
				if err != nil {
					t.Error(err)
					return
				}
				ids <- r.OrderID
			}
		}()
	}
	wg.Wait()
	close(ids)
	seen := map[book.OrderID]bool{}
	for id := range ids {
		if seen[id] {
			t.Fatalf("order ID %d assigned twice", id)
		}
		seen[id] = true
	}
	snap, _ := e.Snapshot(ctx, "PETR4", 0)
	if snap.Seq != clients*perClient {
		t.Errorf("seq = %d, want %d", snap.Seq, clients*perClient)
	}
}

// A consumer that applies every update to its snapshot ends with the same book as a
// fresh snapshot, and sequence numbers have no gaps.
func TestMarketDataRebuildsTheBook(t *testing.T) {
	e := open(t, "", "PETR4")
	defer e.Close()
	drive(t, e, "PETR4", 5, 500) // some state before subscribing
	sub, err := e.Subscribe(ctx, "PETR4", 1<<16)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	var wg sync.WaitGroup
	for seed := range uint64(4) {
		wg.Add(1)
		go func() { defer wg.Done(); drive(t, e, "PETR4", 10+seed, 2_000) }()
	}
	wg.Wait()
	final, _ := e.Snapshot(ctx, "PETR4", 0) // every update was published before this

	type key struct {
		side  book.Side
		price int64
	}
	depth := map[key]int64{}
	first := <-sub.C
	if !first.Snapshot {
		t.Fatal("first message is not a snapshot")
	}
	for _, l := range first.Bids {
		depth[key{book.Buy, l.Price}] = l.Qty
	}
	for _, l := range first.Asks {
		depth[key{book.Sell, l.Price}] = l.Qty
	}
	seq, trades := first.Seq, 0
	for done := false; !done; {
		select {
		case md := <-sub.C:
			if md.Seq != seq+1 {
				t.Fatalf("gap: %d after %d", md.Seq, seq)
			}
			seq = md.Seq
			trades += len(md.Trades)
			for _, c := range md.Levels {
				if c.Qty == 0 {
					delete(depth, key{c.Side, c.Price})
				} else {
					depth[key{c.Side, c.Price}] = c.Qty
				}
			}
		default:
			done = true
		}
	}
	want := map[key]int64{}
	for _, l := range final.Bids {
		want[key{book.Buy, l.Price}] = l.Qty
	}
	for _, l := range final.Asks {
		want[key{book.Sell, l.Price}] = l.Qty
	}
	if len(depth) != len(want) {
		t.Fatalf("rebuilt book has %d levels, snapshot %d", len(depth), len(want))
	}
	for k, q := range want {
		if depth[k] != q {
			t.Fatalf("level %v: rebuilt %d, snapshot %d", k, depth[k], q)
		}
	}
	if trades == 0 {
		t.Error("no trades seen")
	}
}

func TestSlowConsumerIsDropped(t *testing.T) {
	e := open(t, "", "PETR4")
	defer e.Close()
	sub, _ := e.Subscribe(ctx, "PETR4", 2) // snapshot + 1 update fit
	for i := range 5 {
		e.Submit(ctx, "PETR4", limit(book.Buy, 100+int64(i), 1))
	}
	n := 0
	for range sub.C {
		n++
	}
	if n != 2 || !errors.Is(sub.Err(), ErrSlowConsumer) {
		t.Fatalf("got %d messages, err %v", n, sub.Err())
	}
	if _, err := e.Submit(ctx, "PETR4", limit(book.Buy, 1, 1)); err != nil {
		t.Fatalf("matching must not be affected: %v", err)
	}
}

func TestCloseEndsSubscriptions(t *testing.T) {
	e := open(t, "", "PETR4")
	sub, _ := e.Subscribe(ctx, "PETR4", 8)
	<-sub.C // snapshot
	e.Close()
	if _, open := <-sub.C; open || !errors.Is(sub.Err(), ErrStopped) {
		t.Fatalf("subscription still open or wrong error: %v", sub.Err())
	}
	sub.Close() // safe after the engine stopped
}

func TestCommandEncodingRoundTrip(t *testing.T) {
	for _, c := range []Command{
		{Kind: KindSubmit, Time: 1_700_000_000_000_000_000, Order: book.Order{
			ID: 1 << 40, Owner: 42, Side: book.Sell, Type: book.Limit, TIF: book.FOK, Price: 3415, Qty: 7, PostOnly: false, STP: book.CancelBoth,
		}},
		{Kind: KindSubmit, Order: book.Order{ID: 2, Side: book.Buy, Type: book.Limit, TIF: book.GTC, Price: 1, Qty: 1, PostOnly: true}},
		{Kind: KindCancel, ID: 99, Time: 5},
		{Kind: KindModify, ID: 7, Price: 101, Qty: 3, Time: 6},
	} {
		raw := c.appendBinary(nil)
		got, err := decodeCommand(raw)
		if err != nil || got != c {
			t.Errorf("round trip of %+v: got %+v, %v", c, got, err)
		}
	}
	if _, err := decodeCommand(make([]byte, commandSize)); err == nil {
		t.Error("version 0 accepted")
	}
	if _, err := decodeCommand([]byte{1, 2}); err == nil {
		t.Error("short record accepted")
	}
}

type brokenWAL struct{}

func (brokenWAL) Add([]byte) error { return nil }
func (brokenWAL) Commit() error    { return errors.New("input/output error") }
func (brokenWAL) Size() int64      { return 0 }
func (brokenWAL) Close() error     { return nil }
