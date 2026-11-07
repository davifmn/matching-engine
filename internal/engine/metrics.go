package engine

import "github.com/davifmn/matching-engine/internal/metrics"

type engineMetrics struct {
	commands     metrics.CounterVec
	rejects      metrics.CounterVec
	trades       metrics.CounterVec
	lots         metrics.CounterVec
	resting      metrics.GaugeVec
	subscribers  metrics.GaugeVec
	slow         metrics.CounterVec
	batch        metrics.HistogramVec
	commit       metrics.HistogramVec
	latency      metrics.HistogramVec
	journalBytes metrics.GaugeVec
}

func newEngineMetrics(r *metrics.Registry) *engineMetrics {
	return &engineMetrics{
		commands:     r.Counter("matching_commands_total", "Commands applied to the book.", "instrument", "kind"),
		rejects:      r.Counter("matching_rejects_total", "Requests rejected by the book.", "instrument", "reason"),
		trades:       r.Counter("matching_trades_total", "Trades.", "instrument"),
		lots:         r.Counter("matching_traded_lots_total", "Quantity traded, in lots.", "instrument"),
		resting:      r.Gauge("matching_resting_orders", "Orders resting on the book.", "instrument"),
		subscribers:  r.Gauge("matching_market_data_subscribers", "Open market data subscriptions.", "instrument"),
		slow:         r.Counter("matching_slow_consumers_total", "Subscribers dropped for falling behind.", "instrument"),
		batch:        r.Histogram("matching_batch_commands", "Commands per journal commit (group commit size).", metrics.SizeBuckets(), "instrument"),
		commit:       r.Histogram("matching_journal_commit_seconds", "Time to write and fsync one batch.", metrics.LatencyBuckets(), "instrument"),
		latency:      r.Histogram("matching_command_seconds", "From enqueue to reply: queueing, journal and matching.", metrics.LatencyBuckets(), "instrument"),
		journalBytes: r.Gauge("matching_journal_bytes", "Size of the journal file.", "instrument"),
	}
}

type shardMetrics struct {
	commands     [KindModify + 1]*metrics.Counter
	rejects      metrics.CounterVec
	trades       *metrics.Counter
	lots         *metrics.Counter
	resting      *metrics.Gauge
	subscribers  *metrics.Gauge
	slow         *metrics.Counter
	batch        *metrics.Histogram
	commit       *metrics.Histogram
	latency      *metrics.Histogram
	journalBytes *metrics.Gauge
}

// forInstrument resolves the labeled series once, so the hot path does no map lookups
// (rejections, keyed by reason, are the exception).
func (m *engineMetrics) forInstrument(sym string) shardMetrics {
	sm := shardMetrics{
		rejects:      m.rejects,
		trades:       m.trades.With(sym),
		lots:         m.lots.With(sym),
		resting:      m.resting.With(sym),
		subscribers:  m.subscribers.With(sym),
		slow:         m.slow.With(sym),
		batch:        m.batch.With(sym),
		commit:       m.commit.With(sym),
		latency:      m.latency.With(sym),
		journalBytes: m.journalBytes.With(sym),
	}
	for _, k := range []Kind{KindSubmit, KindCancel, KindModify} {
		sm.commands[k] = m.commands.With(sym, k.String())
	}
	return sm
}
