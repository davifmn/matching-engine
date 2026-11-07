// Package engine runs one order book per instrument behind a write-ahead journal.
//
// Each instrument is owned by a single goroutine (docs/adr/0001-single-writer.md).
// Requests reach it through a bounded queue; it takes whatever is queued, up to MaxBatch,
// writes those commands to the journal with one write and one fsync (group commit,
// docs/adr/0004-journal.md), and only then applies them to the book and answers. On
// start-up it replays the journal, so a restart rebuilds exactly the same book.
package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/davifmn/matching-engine/internal/book"
	"github.com/davifmn/matching-engine/internal/journal"
	"github.com/davifmn/matching-engine/internal/metrics"
)

var (
	ErrUnknownInstrument = errors.New("unknown instrument")
	// ErrStopped means the instrument is no longer running: the engine was closed, or the
	// journal failed and the instrument stopped rather than acknowledge commands it could
	// not make durable. Treat the outcome of an order that got this error as unknown.
	ErrStopped = errors.New("instrument stopped")
	// ErrSlowConsumer closes a market data subscription that fell too far behind.
	ErrSlowConsumer = errors.New("market data subscriber too slow")
)

// Config configures an Engine.
type Config struct {
	Dir       string           // journal directory; empty keeps everything in memory
	Sync      journal.SyncMode // when to fsync the journal
	QueueSize int              // requests waiting per instrument; default 4096
	MaxBatch  int              // commands per journal commit; default 256
	Metrics   *metrics.Registry
	Logger    *slog.Logger
	Now       func() time.Time // clock for command timestamps; default time.Now
}

// Result is the outcome of one command.
type Result struct {
	Seq     uint64       // position of the command in the instrument's journal, from 1
	OrderID book.OrderID // the order the command was about; new orders get their ID here
	Events  []book.Event
}

// Snapshot is the state of one book at a point in its command sequence.
type Snapshot struct {
	Symbol     string
	Seq        uint64 // commands applied so far
	Bids, Asks []book.PriceLevel
	Orders     int
	Digest     uint64 // only from Engine.Digest
}

// Engine routes requests to the goroutine of each instrument.
type Engine struct {
	shards  map[string]*shard
	symbols []string
}

// New replays the journal of every instrument and starts them.
func New(cfg Config, symbols ...string) (*Engine, error) {
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 4096
	}
	if cfg.MaxBatch <= 0 {
		cfg.MaxBatch = 256
	}
	if cfg.Metrics == nil {
		cfg.Metrics = metrics.NewRegistry()
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	m := newEngineMetrics(cfg.Metrics)
	e := &Engine{shards: make(map[string]*shard, len(symbols))}
	for _, sym := range symbols {
		if _, dup := e.shards[sym]; dup {
			e.Close()
			return nil, fmt.Errorf("instrument %s listed twice", sym)
		}
		s, err := newShard(sym, cfg, m)
		if err != nil {
			e.Close()
			return nil, err
		}
		e.shards[sym] = s
		e.symbols = append(e.symbols, sym)
		go s.run()
	}
	return e, nil
}

// Symbols returns the instruments, in configuration order.
func (e *Engine) Symbols() []string { return slices.Clone(e.symbols) }

func (e *Engine) shard(symbol string) (*shard, error) {
	s, ok := e.shards[symbol]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrUnknownInstrument, symbol)
	}
	return s, nil
}

// Submit sends a new order. The engine assigns its ID (any ID in o is ignored).
func (e *Engine) Submit(ctx context.Context, symbol string, o book.Order) (Result, error) {
	return e.command(ctx, symbol, Command{Kind: KindSubmit, Order: o})
}

// Cancel cancels a resting order.
func (e *Engine) Cancel(ctx context.Context, symbol string, id book.OrderID) (Result, error) {
	return e.command(ctx, symbol, Command{Kind: KindCancel, ID: id})
}

// Modify changes the price and open quantity of a resting order (see book.Book.Modify).
func (e *Engine) Modify(ctx context.Context, symbol string, id book.OrderID, price, qty int64) (Result, error) {
	return e.command(ctx, symbol, Command{Kind: KindModify, ID: id, Price: price, Qty: qty})
}

func (e *Engine) command(ctx context.Context, symbol string, c Command) (Result, error) {
	s, err := e.shard(symbol)
	if err != nil {
		return Result{}, err
	}
	rep, err := s.call(ctx, &request{op: opCommand, cmd: c})
	return rep.res, err
}

// Snapshot returns up to depth levels per side (0 = all), consistent with Seq.
func (e *Engine) Snapshot(ctx context.Context, symbol string, depth int) (Snapshot, error) {
	return e.snapshot(ctx, symbol, depth, false)
}

// Digest returns a full snapshot including the hash of the whole book state.
func (e *Engine) Digest(ctx context.Context, symbol string) (Snapshot, error) {
	return e.snapshot(ctx, symbol, 0, true)
}

func (e *Engine) snapshot(ctx context.Context, symbol string, depth int, digest bool) (Snapshot, error) {
	s, err := e.shard(symbol)
	if err != nil {
		return Snapshot{}, err
	}
	rep, err := s.call(ctx, &request{op: opSnapshot, depth: depth, digest: digest})
	return rep.snap, err
}

// Lookup returns a resting order. found is false for orders that filled, were canceled,
// or never existed: the engine keeps no order history.
func (e *Engine) Lookup(ctx context.Context, symbol string, id book.OrderID) (info book.OrderInfo, found bool, err error) {
	s, err := e.shard(symbol)
	if err != nil {
		return book.OrderInfo{}, false, err
	}
	rep, err := s.call(ctx, &request{op: opLookup, cmd: Command{ID: id}})
	return rep.info, rep.found, err
}

// Ready returns an error if any instrument has stopped, from the moment it decided to
// stop: a client that got ErrStopped never sees Ready() == nil afterwards.
func (e *Engine) Ready() error {
	for _, sym := range e.symbols {
		if s := e.shards[sym]; s.dead.Load() {
			return s.err
		}
	}
	return nil
}

// Close stops every instrument after the requests already queued, closes the journals
// and ends the market data subscriptions.
func (e *Engine) Close() error {
	var errs []error
	for _, s := range e.shards {
		s.closeOnce.Do(func() { close(s.quit) })
	}
	for _, s := range e.shards {
		<-s.stopped
		if s.closeErr != nil {
			errs = append(errs, s.closeErr)
		}
	}
	return errors.Join(errs...)
}

type opKind uint8

const (
	opCommand opKind = iota
	opSnapshot
	opSubscribe
	opUnsubscribe
	opLookup
)

type request struct {
	op       opKind
	cmd      Command
	seq      uint64
	depth    int
	digest   bool
	sub      *Subscription
	reply    chan reply // buffered; nil when nobody waits
	enqueued time.Time
}

// wal is the part of journal.Writer the engine uses; tests swap in a failing one.
type wal interface {
	Add(payload []byte) error
	Commit() error
	Size() int64
	Close() error
}

type reply struct {
	res   Result
	snap  Snapshot
	info  book.OrderInfo
	found bool
	err   error
}

// shard is one instrument: its book, journal and subscribers, all owned by run.
type shard struct {
	symbol   string
	in       chan *request
	quit     chan struct{}
	stopped  chan struct{}
	dead     atomic.Bool // set, after err, as soon as the shard decides to stop
	err      error       // why it stopped; read only after seeing dead or stopped
	closeErr error

	closeOnce sync.Once

	book     *book.Book
	seq      uint64
	nextID   book.OrderID
	wal      wal // nil when the engine runs in memory
	buf      []byte
	maxBatch int
	now      func() time.Time
	log      *slog.Logger

	mdSeq uint64
	subs  map[*Subscription]struct{}

	m shardMetrics
}

func newShard(symbol string, cfg Config, m *engineMetrics) (*shard, error) {
	s := &shard{
		symbol:   symbol,
		in:       make(chan *request, cfg.QueueSize),
		quit:     make(chan struct{}),
		stopped:  make(chan struct{}),
		book:     book.New(),
		maxBatch: cfg.MaxBatch,
		now:      cfg.Now,
		log:      cfg.Logger.With("instrument", symbol),
		subs:     map[*Subscription]struct{}{},
		m:        m.forInstrument(symbol),
	}
	if cfg.Dir != "" {
		path := filepath.Join(cfg.Dir, symbol+".wal")
		start := time.Now()
		st, err := journal.Replay(path, s.replay)
		if err != nil {
			return nil, fmt.Errorf("%s: replaying %s: %w", symbol, path, err)
		}
		if st.TruncatedBytes > 0 {
			s.log.Warn("removed a torn record at the end of the journal", "bytes", st.TruncatedBytes)
		}
		s.log.Info("journal replayed", "commands", st.Records, "bytes", st.Bytes,
			"resting_orders", s.book.Len(), "took", time.Since(start))
		w, err := journal.Open(path, cfg.Sync)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", symbol, err)
		}
		s.wal = w
		s.m.journalBytes.Set(float64(s.wal.Size()))
	}
	s.m.resting.Set(float64(s.book.Len()))
	return s, nil
}

func (s *shard) replay(payload []byte) error {
	c, err := decodeCommand(payload)
	if err != nil {
		return err
	}
	s.seq++
	if c.Kind == KindSubmit {
		s.nextID = max(s.nextID, c.Order.ID)
	}
	s.apply(&c)
	return nil
}

func (s *shard) call(ctx context.Context, r *request) (reply, error) {
	if s.dead.Load() {
		return reply{}, s.err
	}
	r.reply = make(chan reply, 1)
	r.enqueued = time.Now()
	select {
	case s.in <- r:
	case <-s.stopped:
		return reply{}, s.stopErr()
	case <-ctx.Done():
		return reply{}, ctx.Err()
	}
	// Once queued the request will be handled, so wait for it rather than for ctx.
	select {
	case rep := <-r.reply:
		return rep, rep.err
	case <-s.stopped:
		select { // the reply is sent before stopped is closed
		case rep := <-r.reply:
			return rep, rep.err
		default:
			return reply{}, s.stopErr()
		}
	}
}

// markDead records why the shard stops. Only the shard goroutine calls it.
func (s *shard) markDead(err error) {
	if !s.dead.Load() {
		s.err = err
		s.dead.Store(true)
	}
}

func (s *shard) stopErr() error {
	if s.dead.Load() {
		return s.err
	}
	return fmt.Errorf("%s: %w", s.symbol, ErrStopped)
}

func (s *shard) run() {
	defer s.shutdown()
	batch := make([]*request, 0, s.maxBatch)
	for {
		select {
		case <-s.quit:
			return
		case r := <-s.in:
			batch = append(batch[:0], r)
		drain:
			for len(batch) < s.maxBatch {
				select {
				case r := <-s.in:
					batch = append(batch, r)
				default:
					break drain
				}
			}
			if err := s.process(batch); err != nil {
				return // process marked the shard dead
			}
			clear(batch)
		}
	}
}

func (s *shard) shutdown() {
	s.markDead(fmt.Errorf("%s: %w", s.symbol, ErrStopped))
	err := s.err
	for {
		select {
		case r := <-s.in:
			if r.reply != nil {
				r.reply <- reply{err: err}
			}
			continue
		default:
		}
		break
	}
	for sub := range s.subs {
		s.drop(sub, ErrStopped)
	}
	if s.wal != nil {
		s.closeErr = s.wal.Close()
	}
	close(s.stopped)
}

// process journals the commands of a batch with one commit, then applies the batch in
// order. If the commit fails, nothing is applied and the instrument stops.
func (s *shard) process(batch []*request) error {
	now := s.now().UnixNano()
	commands := 0
	for _, r := range batch {
		if r.op != opCommand {
			continue
		}
		commands++
		s.seq++
		r.seq = s.seq
		r.cmd.Time = now
		if r.cmd.Kind == KindSubmit {
			s.nextID++
			r.cmd.Order.ID = s.nextID
		}
		if s.wal != nil {
			s.buf = r.cmd.appendBinary(s.buf[:0])
			if err := s.wal.Add(s.buf); err != nil {
				return s.failBatch(batch, err)
			}
		}
	}
	if commands > 0 {
		s.m.batch.Observe(float64(commands))
		if s.wal != nil {
			start := time.Now()
			if err := s.wal.Commit(); err != nil {
				return s.failBatch(batch, err)
			}
			s.m.commit.ObserveDuration(time.Since(start))
			s.m.journalBytes.Set(float64(s.wal.Size()))
		}
	}
	for _, r := range batch {
		s.handle(r)
	}
	s.m.resting.Set(float64(s.book.Len()))
	return nil
}

func (s *shard) failBatch(batch []*request, cause error) error {
	err := fmt.Errorf("%s: %w: journal: %v", s.symbol, ErrStopped, cause)
	s.log.Error("journal failed, stopping the instrument", "error", cause)
	s.markDead(err) // before any reply, so Ready() already fails when clients see the error
	for _, r := range batch {
		if r.reply != nil {
			r.reply <- reply{err: err}
		}
	}
	return err
}

func (s *shard) handle(r *request) {
	switch r.op {
	case opCommand:
		ev := s.apply(&r.cmd)
		s.observe(r.cmd.Kind, ev)
		if len(s.subs) > 0 {
			s.publish(ev, r.cmd.Time)
		}
		id := r.cmd.ID
		if r.cmd.Kind == KindSubmit {
			id = r.cmd.Order.ID
		}
		r.reply <- reply{res: Result{Seq: r.seq, OrderID: id, Events: slices.Clone(ev)}}
		s.m.latency.ObserveDuration(time.Since(r.enqueued))
	case opSnapshot:
		r.reply <- reply{snap: s.snapshot(r.depth, r.digest)}
	case opLookup:
		info, found := s.book.Lookup(r.cmd.ID)
		r.reply <- reply{info: info, found: found}
	case opSubscribe:
		snap := s.snapshot(0, false)
		// The channel is new and has room: the snapshot always goes first.
		r.sub.ch <- MarketData{Seq: s.mdSeq, Snapshot: true, Bids: snap.Bids, Asks: snap.Asks, Time: s.now().UnixNano()}
		s.subs[r.sub] = struct{}{}
		s.m.subscribers.Set(float64(len(s.subs)))
		r.reply <- reply{}
	case opUnsubscribe:
		if _, ok := s.subs[r.sub]; ok {
			s.drop(r.sub, nil)
		}
	}
}

func (s *shard) apply(c *Command) []book.Event {
	switch c.Kind {
	case KindSubmit:
		return s.book.Submit(c.Order)
	case KindCancel:
		return s.book.Cancel(c.ID)
	default:
		return s.book.Modify(c.ID, c.Price, c.Qty)
	}
}

func (s *shard) snapshot(depth int, digest bool) Snapshot {
	snap := Snapshot{
		Symbol: s.symbol,
		Seq:    s.seq,
		Bids:   s.book.Depth(book.Buy, depth, nil),
		Asks:   s.book.Depth(book.Sell, depth, nil),
		Orders: s.book.Len(),
	}
	if digest {
		snap.Digest = s.book.Digest()
	}
	return snap
}

func (s *shard) observe(k Kind, ev []book.Event) {
	s.m.commands[k].Inc()
	for _, e := range ev {
		switch e.Kind {
		case book.EventRejected:
			s.m.rejects.With(s.symbol, e.Reason.String()).Inc()
		case book.EventTrade:
			s.m.trades.Inc()
			s.m.lots.Add(uint64(e.Qty))
		}
	}
}
