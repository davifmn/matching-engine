package engine

import (
	"context"
	"slices"
	"sync"

	"github.com/davifmn/matching-engine/internal/book"
)

// Trade is a public trade print: no order IDs or accounts.
type Trade struct {
	Price     int64
	Qty       int64
	TakerSide book.Side
	Time      int64
}

// MarketData is one message of a subscription. The first is a full snapshot; each later
// message carries Seq = previous Seq + 1, so a consumer that sees a gap knows it missed
// something and must resubscribe. Applying Levels (new totals, 0 = removed) to the
// snapshot keeps a full copy of the book.
type MarketData struct {
	Seq        uint64
	Snapshot   bool
	Bids, Asks []book.PriceLevel // snapshot only
	Levels     []book.LevelChange
	Trades     []Trade
	Time       int64
}

// Subscription receives the market data of one instrument.
type Subscription struct {
	C <-chan MarketData // closed when the subscription ends; then see Err

	ch    chan MarketData
	shard *shard
	err   error // set by the shard before it closes ch
	once  sync.Once
}

// Err says why C was closed: ErrSlowConsumer, ErrStopped, or nil after Close.
func (s *Subscription) Err() error { return s.err }

// Close ends the subscription.
func (s *Subscription) Close() {
	s.once.Do(func() {
		select {
		case s.shard.in <- &request{op: opUnsubscribe, sub: s}:
		case <-s.shard.stopped:
		}
	})
}

// Subscribe starts a market data stream. buffer is how many messages may wait for the
// consumer; a consumer that falls further behind is disconnected instead of slowing
// down matching.
func (e *Engine) Subscribe(ctx context.Context, symbol string, buffer int) (*Subscription, error) {
	s, err := e.shard(symbol)
	if err != nil {
		return nil, err
	}
	ch := make(chan MarketData, max(buffer, 2))
	sub := &Subscription{C: ch, ch: ch, shard: s}
	if _, err := s.call(ctx, &request{op: opSubscribe, sub: sub}); err != nil {
		return nil, err
	}
	return sub, nil
}

// publish sends the effect of one command to every subscriber. It never blocks.
func (s *shard) publish(ev []book.Event, ts int64) {
	changes := s.book.Changes()
	var trades []Trade
	for _, e := range ev {
		if e.Kind == book.EventTrade {
			trades = append(trades, Trade{Price: e.Price, Qty: e.Qty, TakerSide: e.Side, Time: ts})
		}
	}
	if len(changes) == 0 && len(trades) == 0 {
		return
	}
	s.mdSeq++
	md := MarketData{Seq: s.mdSeq, Levels: slices.Clone(changes), Trades: trades, Time: ts}
	for sub := range s.subs {
		select {
		case sub.ch <- md:
		default:
			s.m.slow.Inc()
			s.log.Warn("dropping slow market data subscriber")
			s.drop(sub, ErrSlowConsumer)
		}
	}
}

func (s *shard) drop(sub *Subscription, err error) {
	delete(s.subs, sub)
	sub.err = err
	close(sub.ch)
	s.m.subscribers.Set(float64(len(s.subs)))
}
