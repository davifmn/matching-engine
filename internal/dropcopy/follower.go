package dropcopy

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/davifmn/matching-engine/internal/engine"
	"github.com/davifmn/matching-engine/internal/instrument"
	"github.com/davifmn/matching-engine/internal/journal"
	"github.com/davifmn/matching-engine/internal/metrics"
)

// Publisher sends trades to the broker.
type Publisher interface {
	// Publish sends the trades, in order, and returns once the broker has stored all of
	// them. After an error it is called again with the same trades.
	Publish(ctx context.Context, trades []Trade) error
}

// Metrics are shared by the followers of all instruments.
type Metrics struct {
	published metrics.CounterVec
	anonymous metrics.CounterVec
	failures  metrics.CounterVec
	cursor    metrics.GaugeVec
	behind    metrics.GaugeVec
}

func NewMetrics(r *metrics.Registry) *Metrics {
	return &Metrics{
		published: r.Counter("dropcopy_trades_published_total", "Trades published to the broker.", "instrument"),
		anonymous: r.Counter("dropcopy_trades_anonymous_total", "Trades not published because a side has no account.", "instrument"),
		failures:  r.Counter("dropcopy_publish_failures_total", "Failed publish attempts (each is retried).", "instrument"),
		cursor:    r.Gauge("dropcopy_cursor_seq", "Last journal command whose trades are all published.", "instrument"),
		behind:    r.Gauge("dropcopy_journal_bytes_behind", "Journal bytes written by the engine and not yet read.", "instrument"),
	}
}

// Config configures a Follower.
type Config struct {
	Spec      instrument.Spec
	Dir       string // the engine's journal directory (read only)
	StateDir  string // where the cursor is kept
	Publisher Publisher
	Poll      time.Duration // how often to look for new records when idle; default 50ms
	MaxBatch  int           // journal records per read; default 1000
	Metrics   *Metrics
	Logger    *slog.Logger
}

// Follower publishes the trades of one instrument.
type Follower struct {
	cfg     Config
	path    string
	replica *engine.Replica
	cursor  cursor
	offset  int64  // in the journal file
	done    uint64 // last command whose trades are published (persisted in the cursor)
}

func NewFollower(cfg Config) (*Follower, error) {
	if cfg.Spec.Currency == "" {
		return nil, fmt.Errorf("%s: the instrument has no currency, so its trades cannot be settled", cfg.Spec.Symbol)
	}
	if cfg.Poll <= 0 {
		cfg.Poll = 50 * time.Millisecond
	}
	if cfg.MaxBatch <= 0 {
		cfg.MaxBatch = 1000
	}
	if cfg.Metrics == nil {
		cfg.Metrics = NewMetrics(metrics.NewRegistry())
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	cfg.Logger = cfg.Logger.With("instrument", cfg.Spec.Symbol)
	return &Follower{
		cfg:     cfg,
		path:    filepath.Join(cfg.Dir, cfg.Spec.Symbol+".wal"),
		replica: engine.NewReplica(),
		cursor:  newCursor(cfg.StateDir, cfg.Spec.Symbol),
	}, nil
}

// Run replays the journal from the beginning (the book must be rebuilt to know what
// traded), publishes the trades of the commands after the cursor, then keeps following
// the journal until ctx is done.
//
// Delivery is at least once: a crash after the broker stored a batch and before the
// cursor moved publishes that batch again on restart, with the same trade ids.
func (f *Follower) Run(ctx context.Context) error {
	done, err := f.cursor.load()
	if err != nil {
		return err
	}
	f.done = done
	f.cfg.Metrics.cursor.With(f.cfg.Spec.Symbol).Set(float64(done))
	f.cfg.Logger.Info("following the journal", "path", f.path, "published_up_to_seq", done)
	for {
		progressed, err := f.step(ctx)
		if errors.Is(err, context.Canceled) || ctx.Err() != nil {
			return nil
		}
		if err != nil {
			return fmt.Errorf("%s: %w", f.cfg.Spec.Symbol, err)
		}
		if !progressed {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(f.cfg.Poll):
			}
		}
	}
}

// step reads up to MaxBatch journal records, publishes the new trades and moves the
// cursor. It reports whether it read anything.
func (f *Follower) step(ctx context.Context) (bool, error) {
	var batch []Trade
	var last uint64
	anonymous := 0
	next, err := journal.ReadFrom(f.path, f.offset, f.cfg.MaxBatch, func(p []byte) error {
		seq, c, events, err := f.replica.Apply(p)
		if err != nil {
			return err
		}
		last = seq
		if seq <= f.done {
			return nil // published before a restart; replayed only to rebuild the book
		}
		trades, anon := Trades(f.cfg.Spec, seq, c, events)
		batch = append(batch, trades...)
		anonymous += anon
		return nil
	})
	if err != nil {
		return false, err
	}
	if next == f.offset {
		return false, f.idle()
	}
	if len(batch) > 0 {
		if err := f.publish(ctx, batch); err != nil {
			return false, err
		}
	}
	f.offset = next
	sym := f.cfg.Spec.Symbol
	f.cfg.Metrics.anonymous.With(sym).Add(uint64(anonymous))
	if last > f.done {
		if err := f.cursor.save(last); err != nil {
			return false, fmt.Errorf("saving the cursor: %w", err)
		}
		f.done = last
		f.cfg.Metrics.cursor.With(sym).Set(float64(last))
	}
	return true, nil
}

// idle runs when the follower has read everything there is.
func (f *Follower) idle() error {
	if f.replica.Seq() < f.done {
		// The cursor says trades up to command N were published, but the journal has
		// fewer commands: it was replaced. Publishing on would skip or repeat trades.
		return fmt.Errorf("the journal has %d commands but trades up to command %d were already published: was the journal replaced?",
			f.replica.Seq(), f.done)
	}
	if info, err := os.Stat(f.path); err == nil {
		f.cfg.Metrics.behind.With(f.cfg.Spec.Symbol).Set(float64(info.Size() - f.offset))
	}
	return nil
}

// publish retries until the broker takes the batch or ctx ends. Moving on without it
// would lose trades, so a broker outage stops the follower here, with
// dropcopy_journal_bytes_behind growing.
func (f *Follower) publish(ctx context.Context, batch []Trade) error {
	sym := f.cfg.Spec.Symbol
	backoff := 100 * time.Millisecond
	for {
		err := f.cfg.Publisher.Publish(ctx, batch)
		if err == nil {
			f.cfg.Metrics.published.With(sym).Add(uint64(len(batch)))
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		f.cfg.Metrics.failures.With(sym).Inc()
		f.cfg.Logger.Warn("publish failed, retrying", "trades", len(batch), "error", err, "retry_in", backoff)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(2*backoff, 10*time.Second)
	}
}
