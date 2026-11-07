package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/davifmn/matching-engine/internal/engine"
	"github.com/davifmn/matching-engine/internal/instrument"
)

type levelChange struct {
	Side     string `json:"side"`
	Price    string `json:"price"`
	Quantity string `json:"quantity"` // new total; "0" means the level is gone
}

type tradePrint struct {
	Price     string `json:"price"`
	Quantity  string `json:"quantity"`
	TakerSide string `json:"taker_side"`
	Time      string `json:"time"`
}

type snapshotEvent struct {
	Seq  uint64  `json:"seq"`
	Bids []level `json:"bids"`
	Asks []level `json:"asks"`
}

type updateEvent struct {
	Seq    uint64        `json:"seq"`
	Time   string        `json:"time"`
	Levels []levelChange `json:"levels,omitempty"`
	Trades []tradePrint  `json:"trades,omitempty"`
}

// stream serves market data as Server-Sent Events: one "snapshot" with the full book,
// then an "update" per change with consecutive seq numbers. A client that sees a gap,
// or an "error" event, reconnects and starts again from a new snapshot.
func (s *Server) stream(w http.ResponseWriter, r *http.Request) {
	sp, ok := s.spec(w, r)
	if !ok {
		return
	}
	sub, err := s.eng.Subscribe(r.Context(), sp.Symbol, s.StreamBuffer)
	if err != nil {
		s.engineError(w, err)
		return
	}
	defer sub.Close()

	rc := http.NewResponseController(w)
	_ = rc.SetWriteDeadline(time.Time{}) // streams outlive the server's write timeout
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("X-Accel-Buffering", "no") // tell nginx-style proxies not to buffer
	w.WriteHeader(http.StatusOK)

	heartbeat := time.NewTicker(s.Heartbeat)
	defer heartbeat.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-s.streams:
			writeEvent(w, "error", 0, map[string]string{"error": "server shutting down"})
			_ = rc.Flush()
			return
		case <-heartbeat.C:
			if _, err := io.WriteString(w, ": keep-alive\n\n"); err != nil {
				return
			}
		case md, open := <-sub.C:
			if !open {
				reason := "stream ended"
				if errors.Is(sub.Err(), engine.ErrSlowConsumer) {
					reason = "slow consumer: too far behind, reconnect"
				} else if errors.Is(sub.Err(), engine.ErrStopped) {
					reason = "instrument stopped"
				}
				writeEvent(w, "error", 0, map[string]string{"error": reason})
				_ = rc.Flush()
				return
			}
			var err error
			if md.Snapshot {
				err = writeEvent(w, "snapshot", md.Seq, snapshotEvent{md.Seq, levels(sp, md.Bids), levels(sp, md.Asks)})
			} else {
				err = writeEvent(w, "update", md.Seq, toUpdate(sp, md))
			}
			if err != nil {
				return
			}
		}
		if err := rc.Flush(); err != nil {
			return
		}
	}
}

func toUpdate(sp instrument.Spec, md engine.MarketData) updateEvent {
	u := updateEvent{Seq: md.Seq, Time: formatTime(md.Time)}
	for _, c := range md.Levels {
		u.Levels = append(u.Levels, levelChange{c.Side.String(), sp.Price(c.Price), sp.Qty(c.Qty)})
	}
	for _, t := range md.Trades {
		u.Trades = append(u.Trades, tradePrint{sp.Price(t.Price), sp.Qty(t.Qty), t.TakerSide.String(), formatTime(t.Time)})
	}
	return u
}

func formatTime(ns int64) string { return time.Unix(0, ns).UTC().Format(time.RFC3339Nano) }

func writeEvent(w io.Writer, event string, id uint64, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if id > 0 {
		_, err = fmt.Fprintf(w, "event: %s\nid: %d\ndata: %s\n\n", event, id, data)
	} else {
		_, err = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, data)
	}
	return err
}
