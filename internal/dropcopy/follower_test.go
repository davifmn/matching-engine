package dropcopy

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/davifmn/matching-engine/internal/book"
	"github.com/davifmn/matching-engine/internal/engine"
	"github.com/davifmn/matching-engine/internal/journal"
)

var ctx = context.Background()

// broker records what was published. It can be told to fail the next n attempts.
type broker struct {
	mu        sync.Mutex
	trades    []Trade
	failNext  int
	attempts  int
	published chan struct{}
}

func newBroker() *broker { return &broker{published: make(chan struct{}, 1000)} }

func (b *broker) Publish(_ context.Context, trades []Trade) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.attempts++
	if b.failNext > 0 {
		b.failNext--
		return errors.New("broker unavailable")
	}
	b.trades = append(b.trades, trades...)
	b.published <- struct{}{}
	return nil
}

func (b *broker) ids() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	ids := make([]string, len(b.trades))
	for i, t := range b.trades {
		ids[i] = t.TradeID
	}
	return ids
}

func (b *broker) waitFor(t *testing.T, n int) {
	t.Helper()
	deadline := time.After(10 * time.Second)
	for len(b.ids()) < n {
		select {
		case <-b.published:
		case <-deadline:
			t.Fatalf("published %d trades, want %d", len(b.ids()), n)
		}
	}
}

type exchange struct {
	t        *testing.T
	dir      string
	eng      *engine.Engine
	expected []string // trade ids, from the engine's own results
}

func newExchange(t *testing.T) *exchange {
	x := &exchange{t: t, dir: t.TempDir()}
	x.start()
	return x
}

func (x *exchange) start() {
	e, err := engine.New(engine.Config{Dir: x.dir, Sync: journal.SyncNever}, "SAP")
	if err != nil {
		x.t.Fatal(err)
	}
	x.eng = e
}

func (x *exchange) restart() {
	if err := x.eng.Close(); err != nil {
		x.t.Fatal(err)
	}
	x.start()
}

// cross rests a sell from one account and hits it with a buy from another, then records
// the trade ids the drop copy must publish, computed from the engine's answer.
func (x *exchange) cross(seller, buyer uint64, price, qty int64) {
	x.t.Helper()
	sell := book.Order{Owner: seller, Side: book.Sell, Type: book.Limit, TIF: book.GTC, Price: price, Qty: qty}
	if _, err := x.eng.Submit(ctx, "SAP", sell); err != nil {
		x.t.Fatal(err)
	}
	buy := book.Order{Owner: buyer, Side: book.Buy, Type: book.Limit, TIF: book.IOC, Price: price, Qty: qty}
	res, err := x.eng.Submit(ctx, "SAP", buy)
	if err != nil {
		x.t.Fatal(err)
	}
	n := 0
	for _, e := range res.Events {
		if e.Kind == book.EventTrade {
			n++
			x.expected = append(x.expected, fmt.Sprintf("SAP-%d-%d", res.Seq, n))
		}
	}
}

func run(t *testing.T, x *exchange, state string, b *broker) (stop func() error) {
	t.Helper()
	f, err := NewFollower(Config{Spec: sap, Dir: x.dir, StateDir: state, Publisher: b, Poll: 5 * time.Millisecond, MaxBatch: 7})
	if err != nil {
		t.Fatal(err)
	}
	c, cancel := context.WithCancel(ctx)
	errc := make(chan error, 1)
	go func() { errc <- f.Run(c) }()
	return func() error {
		cancel()
		return <-errc
	}
}

func TestPublishesEveryTradeOnceWithTheEnginesIds(t *testing.T) {
	x := newExchange(t)
	b := newBroker()
	for i := range 20 {
		x.cross(1001, 1002, 13400+int64(i), 10)
	}
	stop := run(t, x, t.TempDir(), b)
	for i := range 20 { // and more while the follower is running
		x.cross(1003, 1001, 13500+int64(i), 5)
	}
	b.waitFor(t, len(x.expected))
	if err := stop(); err != nil {
		t.Fatal(err)
	}

	if got := b.ids(); !slices.Equal(got, x.expected) {
		t.Fatalf("published %d trades:\n%v\nwant %d:\n%v", len(got), got, len(x.expected), x.expected)
	}
	first := b.trades[0]
	if first.BuyerAccount != "1002" || first.SellerAccount != "1001" || first.Price != "134.00" ||
		first.Quantity != "10" || first.Currency != "EUR" || !strings.HasSuffix(first.ExecutedAt, "Z") {
		t.Fatalf("first trade = %+v", first)
	}
}

func TestARestartPublishesOnlyWhatIsNew(t *testing.T) {
	x := newExchange(t)
	state := t.TempDir()
	b := newBroker()
	x.cross(1001, 1002, 100, 1)
	x.cross(1001, 1002, 101, 1)
	stop := run(t, x, state, b)
	b.waitFor(t, 2)
	if err := stop(); err != nil {
		t.Fatal(err)
	}

	x.cross(1001, 1002, 102, 1)
	stop = run(t, x, state, b)
	b.waitFor(t, 3)
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	if got := b.ids(); !slices.Equal(got, x.expected) {
		t.Fatalf("after a restart: %v, want %v", got, x.expected)
	}
}

// A crash after the broker stored a batch but before the cursor was saved: the batch is
// published again, with the same ids, and nothing is lost.
func TestACrashBeforeTheCursorIsSavedRepublishesWithTheSameIds(t *testing.T) {
	x := newExchange(t)
	state := t.TempDir()
	b := newBroker()
	for i := range 5 {
		x.cross(1001, 1002, 100+int64(i), 1)
	}
	stop := run(t, x, state, b)
	b.waitFor(t, 5)
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	// roll the cursor back to just after the first cross (commands 1 and 2)
	if err := newCursor(state, "SAP").save(2); err != nil {
		t.Fatal(err)
	}

	stop = run(t, x, state, b)
	b.waitFor(t, 9)
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	got := b.ids()
	if !slices.Equal(got[:5], x.expected) || !slices.Equal(got[5:], x.expected[1:]) {
		t.Fatalf("published %v; want the 5 trades, then trades 2-5 again", got)
	}
}

func TestFollowsTheEngineAcrossItsRestart(t *testing.T) {
	x := newExchange(t)
	b := newBroker()
	stop := run(t, x, t.TempDir(), b)
	x.cross(1001, 1002, 100, 1)
	x.restart() // replays its journal and appends to the same file
	x.cross(1001, 1002, 101, 1)
	b.waitFor(t, 2)
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	if got := b.ids(); !slices.Equal(got, x.expected) {
		t.Fatalf("%v, want %v", got, x.expected)
	}
}

func TestABrokerOutageDelaysButLosesNothing(t *testing.T) {
	x := newExchange(t)
	b := newBroker()
	b.failNext = 3
	for i := range 10 {
		x.cross(1001, 1002, 100+int64(i), 1)
	}
	stop := run(t, x, t.TempDir(), b)
	b.waitFor(t, 10)
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	if got := b.ids(); !slices.Equal(got, x.expected) {
		t.Fatalf("%v, want %v", got, x.expected)
	}
	if b.attempts < 4 {
		t.Fatalf("%d attempts: the failed ones were not retried", b.attempts)
	}
}

func TestAnonymousTradesAreNotPublished(t *testing.T) {
	x := newExchange(t)
	b := newBroker()
	x.cross(0, 1002, 100, 1) // anonymous seller
	x.cross(1001, 1002, 101, 1)
	stop := run(t, x, t.TempDir(), b)
	b.waitFor(t, 1)
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	if got := b.ids(); !slices.Equal(got, x.expected[1:]) {
		t.Fatalf("%v, want only %v", got, x.expected[1:])
	}
}

func TestAReplacedJournalStopsTheFollower(t *testing.T) {
	x := newExchange(t)
	state := t.TempDir()
	if err := newCursor(state, "SAP").save(50); err != nil { // published up to command 50 earlier
		t.Fatal(err)
	}
	x.cross(1001, 1002, 100, 1) // but this journal has only 2 commands
	f, err := NewFollower(Config{Spec: sap, Dir: x.dir, StateDir: state, Publisher: newBroker(), Poll: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	c, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := f.Run(c); err == nil || !strings.Contains(err.Error(), "replaced") {
		t.Fatalf("Run = %v, want an error about a replaced journal", err)
	}
}

func TestAnInstrumentWithoutCurrencyIsRefused(t *testing.T) {
	spec := sap
	spec.Currency = ""
	if _, err := NewFollower(Config{Spec: spec}); err == nil {
		t.Fatal("expected an error")
	}
}

func TestTheCursorSurvivesAndStartsAtZero(t *testing.T) {
	dir := t.TempDir()
	c := newCursor(dir, "SAP")
	if seq, err := c.load(); err != nil || seq != 0 {
		t.Fatalf("new cursor = %d, %v", seq, err)
	}
	if err := c.save(123); err != nil {
		t.Fatal(err)
	}
	if seq, err := newCursor(dir, "SAP").load(); err != nil || seq != 123 {
		t.Fatalf("saved cursor = %d, %v", seq, err)
	}
	if err := os.WriteFile(filepath.Join(dir, "SAP.cursor"), []byte("garbage"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := c.load(); err == nil {
		t.Fatal("a damaged cursor must be an error, not zero (that would republish everything)")
	}
}
