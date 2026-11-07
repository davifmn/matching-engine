// Package api is the HTTP interface of the engine: JSON order entry, book snapshots, a
// Server-Sent Events market data stream, health checks and Prometheus metrics.
//
// Prices and quantities travel as decimal strings ("34.15"), never as JSON numbers, and
// are converted exactly into ticks and lots by package instrument.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/davifmn/matching-engine/internal/book"
	"github.com/davifmn/matching-engine/internal/engine"
	"github.com/davifmn/matching-engine/internal/instrument"
	"github.com/davifmn/matching-engine/internal/metrics"
)

const maxBody = 4 << 10

// Server serves the HTTP API.
type Server struct {
	eng   *engine.Engine
	specs map[string]instrument.Spec
	list  []instrument.Spec
	log   *slog.Logger
	reg   *metrics.Registry
	mux   *http.ServeMux

	requests metrics.CounterVec
	duration metrics.HistogramVec

	// Heartbeat is the interval of keep-alive comments on idle streams.
	Heartbeat time.Duration
	// StreamBuffer is how many market data messages may wait for a slow client.
	StreamBuffer int

	streams   chan struct{}
	closeOnce sync.Once
}

// New builds the server. specs must match the engine's instruments.
func New(eng *engine.Engine, specs []instrument.Spec, log *slog.Logger, reg *metrics.Registry) *Server {
	s := &Server{
		eng:          eng,
		specs:        make(map[string]instrument.Spec, len(specs)),
		list:         specs,
		log:          log,
		reg:          reg,
		mux:          http.NewServeMux(),
		requests:     reg.Counter("http_requests_total", "HTTP requests.", "route", "code"),
		duration:     reg.Histogram("http_request_duration_seconds", "HTTP request latency.", metrics.LatencyBuckets(), "route"),
		Heartbeat:    15 * time.Second,
		StreamBuffer: 1024,
		streams:      make(chan struct{}),
	}
	for _, sp := range specs {
		s.specs[sp.Symbol] = sp
	}
	s.mux.HandleFunc("GET /v1/instruments", s.listInstruments)
	s.mux.HandleFunc("POST /v1/instruments/{symbol}/orders", s.submit)
	s.mux.HandleFunc("GET /v1/instruments/{symbol}/orders/{id}", s.lookup)
	s.mux.HandleFunc("PATCH /v1/instruments/{symbol}/orders/{id}", s.modify)
	s.mux.HandleFunc("DELETE /v1/instruments/{symbol}/orders/{id}", s.cancel)
	s.mux.HandleFunc("GET /v1/instruments/{symbol}/book", s.book)
	s.mux.HandleFunc("GET /v1/instruments/{symbol}/stream", s.stream)
	s.mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	s.mux.HandleFunc("GET /readyz", s.ready)
	s.mux.Handle("GET /metrics", reg.Handler())
	return s
}

// CloseStreams ends every market data stream; call it when shutting down, because
// http.Server.Shutdown waits for handlers and streams never finish on their own.
func (s *Server) CloseStreams() { s.closeOnce.Do(func() { close(s.streams) }) }

// Handler wraps the routes with panic recovery, metrics and access logs.
func (s *Server) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		defer func() {
			if p := recover(); p != nil {
				s.log.Error("panic in handler", "panic", p, "method", r.Method, "path", r.URL.Path)
				if !sw.wrote {
					writeError(sw, http.StatusInternalServerError, "internal error")
				}
			}
			route := r.Pattern // set by ServeMux on this request
			if route == "" {
				route = "unmatched"
			}
			took := time.Since(start)
			s.requests.With(route, strconv.Itoa(sw.status)).Inc()
			s.duration.With(route).ObserveDuration(took)
			s.log.Debug("request", "method", r.Method, "path", r.URL.Path, "status", sw.status, "took", took)
		}()
		s.mux.ServeHTTP(sw, r)
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
	wrote  bool
}

func (w *statusWriter) WriteHeader(code int) {
	if !w.wrote {
		w.status, w.wrote = code, true
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	w.wrote = true
	return w.ResponseWriter.Write(b)
}

// Unwrap lets http.ResponseController reach Flush and SetWriteDeadline.
func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// ---- wire types ----

type orderRequest struct {
	Side        string `json:"side"`
	Type        string `json:"type"`
	Price       string `json:"price,omitempty"`
	Quantity    string `json:"quantity"`
	TimeInForce string `json:"time_in_force,omitempty"` // default: gtc for limit, ioc for market
	PostOnly    bool   `json:"post_only,omitempty"`
	// AccountID enables self-trade prevention. In production it would come from the
	// authenticated session, not from the request body.
	AccountID uint64 `json:"account_id,omitempty"`
	STP       string `json:"self_trade_prevention,omitempty"` // default cancel_newest
}

type modifyRequest struct {
	Price    string `json:"price"`
	Quantity string `json:"quantity"` // new open quantity
}

type fill struct {
	Price        string `json:"price"`
	Quantity     string `json:"quantity"`
	MakerOrderID string `json:"maker_order_id"`
}

type orderResponse struct {
	OrderID           string   `json:"order_id"`
	Seq               uint64   `json:"seq"`
	Status            string   `json:"status"` // resting, filled, canceled, rejected
	Reason            string   `json:"reason,omitempty"`
	Price             string   `json:"price,omitempty"` // resting price
	FilledQuantity    string   `json:"filled_quantity"`
	RemainingQuantity string   `json:"remaining_quantity"` // on the book after this request
	CanceledQuantity  string   `json:"canceled_quantity,omitempty"`
	Fills             []fill   `json:"fills"`
	SelfTradeCanceled []string `json:"self_trade_canceled,omitempty"` // own resting orders canceled by STP
}

type level struct {
	Price    string `json:"price"`
	Quantity string `json:"quantity"`
	Orders   int    `json:"orders,omitempty"`
}

type bookResponse struct {
	Symbol        string  `json:"symbol"`
	Seq           uint64  `json:"seq"`
	RestingOrders int     `json:"resting_orders"`
	Bids          []level `json:"bids"`
	Asks          []level `json:"asks"`
}

type restingOrder struct {
	OrderID           string `json:"order_id"`
	Side              string `json:"side"`
	Price             string `json:"price"`
	RemainingQuantity string `json:"remaining_quantity"`
	AccountID         uint64 `json:"account_id,omitempty"`
	PostOnly          bool   `json:"post_only,omitempty"`
	STP               string `json:"self_trade_prevention"`
}

type instrumentResponse struct {
	Symbol      string `json:"symbol"`
	Description string `json:"description,omitempty"`
	TickSize    string `json:"tick_size"`
	LotSize     string `json:"lot_size"`
}

// ---- handlers ----

func (s *Server) listInstruments(w http.ResponseWriter, _ *http.Request) {
	out := make([]instrumentResponse, len(s.list))
	for i, sp := range s.list {
		out[i] = instrumentResponse{sp.Symbol, sp.Description, sp.TickSize.String(), sp.LotSize.String()}
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) spec(w http.ResponseWriter, r *http.Request) (instrument.Spec, bool) {
	sp, ok := s.specs[r.PathValue("symbol")]
	if !ok {
		writeError(w, http.StatusNotFound, "unknown instrument "+strconv.Quote(r.PathValue("symbol")))
	}
	return sp, ok
}

func (s *Server) submit(w http.ResponseWriter, r *http.Request) {
	sp, ok := s.spec(w, r)
	if !ok {
		return
	}
	var req orderRequest
	if !decode(w, r, &req) {
		return
	}
	o, err := parseOrder(sp, req)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	res, err := s.eng.Submit(r.Context(), sp.Symbol, o)
	if err != nil {
		s.engineError(w, err)
		return
	}
	resp := summarize(sp, res)
	writeJSON(w, statusFor(resp, http.StatusCreated), resp)
}

func (s *Server) modify(w http.ResponseWriter, r *http.Request) {
	sp, ok := s.spec(w, r)
	if !ok {
		return
	}
	id, ok := orderID(w, r)
	if !ok {
		return
	}
	var req modifyRequest
	if !decode(w, r, &req) {
		return
	}
	price, err := sp.Ticks(req.Price)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	qty, err := sp.Lots(req.Quantity)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	res, err := s.eng.Modify(r.Context(), sp.Symbol, id, price, qty)
	if err != nil {
		s.engineError(w, err)
		return
	}
	resp := summarize(sp, res)
	writeJSON(w, statusFor(resp, http.StatusOK), resp)
}

func (s *Server) cancel(w http.ResponseWriter, r *http.Request) {
	sp, ok := s.spec(w, r)
	if !ok {
		return
	}
	id, ok := orderID(w, r)
	if !ok {
		return
	}
	res, err := s.eng.Cancel(r.Context(), sp.Symbol, id)
	if err != nil {
		s.engineError(w, err)
		return
	}
	resp := summarize(sp, res)
	writeJSON(w, statusFor(resp, http.StatusOK), resp)
}

func (s *Server) lookup(w http.ResponseWriter, r *http.Request) {
	sp, ok := s.spec(w, r)
	if !ok {
		return
	}
	id, ok := orderID(w, r)
	if !ok {
		return
	}
	info, found, err := s.eng.Lookup(r.Context(), sp.Symbol, id)
	if err != nil {
		s.engineError(w, err)
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, "order is not resting: it filled, was canceled, or does not exist")
		return
	}
	writeJSON(w, http.StatusOK, restingOrder{
		OrderID: formatID(info.ID), Side: info.Side.String(), Price: sp.Price(info.Price),
		RemainingQuantity: sp.Qty(info.Qty), AccountID: info.Owner, PostOnly: info.PostOnly, STP: info.STP.String(),
	})
}

func (s *Server) book(w http.ResponseWriter, r *http.Request) {
	sp, ok := s.spec(w, r)
	if !ok {
		return
	}
	depth := 10
	if v := r.URL.Query().Get("depth"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 1000 {
			writeError(w, http.StatusBadRequest, "depth must be between 1 and 1000")
			return
		}
		depth = n
	}
	snap, err := s.eng.Snapshot(r.Context(), sp.Symbol, depth)
	if err != nil {
		s.engineError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, bookResponse{
		Symbol: sp.Symbol, Seq: snap.Seq, RestingOrders: snap.Orders,
		Bids: levels(sp, snap.Bids), Asks: levels(sp, snap.Asks),
	})
}

func (s *Server) ready(w http.ResponseWriter, _ *http.Request) {
	if err := s.eng.Ready(); err != nil {
		writeError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (s *Server) engineError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, engine.ErrUnknownInstrument):
		writeError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, engine.ErrStopped):
		s.log.Error("engine unavailable", "error", err)
		writeError(w, http.StatusServiceUnavailable, "instrument unavailable; the outcome of this request is unknown, check the order before retrying")
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		writeError(w, http.StatusServiceUnavailable, "request canceled before it reached the engine")
	default:
		s.log.Error("unexpected engine error", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
	}
}

// ---- conversions ----

func parseOrder(sp instrument.Spec, req orderRequest) (book.Order, error) {
	var o book.Order
	switch req.Side {
	case "buy":
		o.Side = book.Buy
	case "sell":
		o.Side = book.Sell
	default:
		return o, errors.New(`side must be "buy" or "sell"`)
	}
	switch req.Type {
	case "limit":
		o.Type, o.TIF = book.Limit, book.GTC
		if req.Price == "" {
			return o, errors.New("limit orders need a price")
		}
		p, err := sp.Ticks(req.Price)
		if err != nil {
			return o, err
		}
		o.Price = p
	case "market":
		o.Type, o.TIF = book.Market, book.IOC
		if req.Price != "" {
			return o, errors.New("market orders take no price")
		}
	default:
		return o, errors.New(`type must be "limit" or "market"`)
	}
	switch req.TimeInForce {
	case "":
	case "gtc":
		o.TIF = book.GTC
	case "ioc":
		o.TIF = book.IOC
	case "fok":
		o.TIF = book.FOK
	default:
		return o, errors.New(`time_in_force must be "gtc", "ioc" or "fok"`)
	}
	switch req.STP {
	case "", "cancel_newest":
		o.STP = book.CancelNewest
	case "cancel_oldest":
		o.STP = book.CancelOldest
	case "cancel_both":
		o.STP = book.CancelBoth
	default:
		return o, errors.New(`self_trade_prevention must be "cancel_newest", "cancel_oldest" or "cancel_both"`)
	}
	if req.Quantity == "" {
		return o, errors.New("quantity is required")
	}
	q, err := sp.Lots(req.Quantity)
	if err != nil {
		return o, err
	}
	o.Qty, o.PostOnly, o.Owner = q, req.PostOnly, req.AccountID
	return o, nil
}

// summarize turns the events of one command into the order's state after it.
func summarize(sp instrument.Spec, res engine.Result) orderResponse {
	resp := orderResponse{OrderID: formatID(res.OrderID), Seq: res.Seq, Fills: []fill{}}
	var filled, remaining, canceled int64
	status := ""
	for _, e := range res.Events {
		mine := e.Order == res.OrderID
		switch e.Kind {
		case book.EventRejected:
			status, resp.Reason = "rejected", e.Reason.String()
		case book.EventModified:
			status, remaining, resp.Price = "resting", e.Qty, sp.Price(e.Price)
		case book.EventTrade:
			filled += e.Qty
			remaining = 0
			status = "filled"
			resp.Fills = append(resp.Fills, fill{sp.Price(e.Price), sp.Qty(e.Qty), formatID(e.Maker)})
		case book.EventRested:
			status, remaining, resp.Price = "resting", e.Qty, sp.Price(e.Price)
		case book.EventCanceled:
			if !mine {
				resp.SelfTradeCanceled = append(resp.SelfTradeCanceled, formatID(e.Order))
				continue
			}
			status, resp.Reason, remaining, canceled = "canceled", e.Reason.String(), 0, e.Qty
		}
	}
	resp.Status = status
	resp.FilledQuantity = sp.Qty(filled)
	resp.RemainingQuantity = sp.Qty(remaining)
	if canceled > 0 {
		resp.CanceledQuantity = sp.Qty(canceled)
	}
	return resp
}

func statusFor(resp orderResponse, ok int) int {
	switch {
	case resp.Status != "rejected":
		return ok
	case resp.Reason == book.ReasonUnknownOrder.String():
		return http.StatusNotFound
	default:
		return http.StatusUnprocessableEntity
	}
}

func levels(sp instrument.Spec, ls []book.PriceLevel) []level {
	out := make([]level, len(ls))
	for i, l := range ls {
		out[i] = level{sp.Price(l.Price), sp.Qty(l.Qty), l.Orders}
	}
	return out
}

func formatID(id book.OrderID) string { return strconv.FormatUint(uint64(id), 10) }

func orderID(w http.ResponseWriter, r *http.Request) (book.OrderID, bool) {
	id, err := strconv.ParseUint(r.PathValue("id"), 10, 64)
	if err != nil || id == 0 {
		writeError(w, http.StatusBadRequest, "order id must be a positive integer")
		return 0, false
	}
	return book.OrderID(id), true
}

func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBody))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid JSON body: %v", err))
		return false
	}
	if dec.More() {
		writeError(w, http.StatusBadRequest, "invalid JSON body: more than one value")
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
