package api

import (
	"bufio"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/davifmn/matching-engine/internal/engine"
	"github.com/davifmn/matching-engine/internal/instrument"
	"github.com/davifmn/matching-engine/internal/metrics"
)

var specs = []instrument.Spec{
	{Symbol: "PETR4", Description: "Petrobras PN", TickSize: instrument.MustDecimal("0.01"), LotSize: instrument.MustDecimal("100")},
	{Symbol: "WINZ26", TickSize: instrument.MustDecimal("5"), LotSize: instrument.MustDecimal("1")},
}

type fixture struct {
	t   *testing.T
	srv *httptest.Server
	api *Server
	eng *engine.Engine
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	reg := metrics.NewRegistry()
	eng, err := engine.New(engine.Config{Metrics: reg}, "PETR4", "WINZ26")
	if err != nil {
		t.Fatal(err)
	}
	a := New(eng, specs, slog.New(slog.DiscardHandler), reg)
	a.Heartbeat = 50 * time.Millisecond
	srv := httptest.NewServer(a.Handler())
	t.Cleanup(func() {
		a.CloseStreams()
		srv.Close()
		eng.Close()
	})
	return &fixture{t, srv, a, eng}
}

// do sends a request and decodes the JSON answer into out (if not nil).
func (f *fixture) do(method, path, body string, wantStatus int, out any) {
	f.t.Helper()
	req, _ := http.NewRequest(method, f.srv.URL+path, strings.NewReader(body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		f.t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != wantStatus {
		f.t.Fatalf("%s %s: status %d, want %d: %s", method, path, resp.StatusCode, wantStatus, raw)
	}
	if out != nil {
		if err := json.Unmarshal(raw, out); err != nil {
			f.t.Fatalf("%s %s: %v: %s", method, path, err, raw)
		}
	}
}

func TestOrderLifecycle(t *testing.T) {
	f := newFixture(t)
	var sell orderResponse
	f.do("POST", "/v1/instruments/PETR4/orders",
		`{"side":"sell","type":"limit","price":"34.15","quantity":"500","account_id":1}`, 201, &sell)
	if sell.Status != "resting" || sell.RemainingQuantity != "500" || sell.Price != "34.15" || sell.OrderID != "1" {
		t.Fatalf("sell: %+v", sell)
	}

	var buy orderResponse
	f.do("POST", "/v1/instruments/PETR4/orders",
		`{"side":"buy","type":"limit","price":"34.20","quantity":"300","account_id":2}`, 201, &buy)
	if buy.Status != "filled" || buy.FilledQuantity != "300" || len(buy.Fills) != 1 ||
		buy.Fills[0] != (fill{"34.15", "300", "1"}) {
		t.Fatalf("buy: %+v", buy)
	}

	var bk bookResponse
	f.do("GET", "/v1/instruments/PETR4/book?depth=5", "", 200, &bk)
	if bk.Seq != 2 || len(bk.Asks) != 1 || bk.Asks[0] != (level{"34.15", "200", 1}) || len(bk.Bids) != 0 {
		t.Fatalf("book: %+v", bk)
	}

	var ro restingOrder
	f.do("GET", "/v1/instruments/PETR4/orders/1", "", 200, &ro)
	if ro.RemainingQuantity != "200" || ro.Side != "sell" || ro.AccountID != 1 {
		t.Fatalf("lookup: %+v", ro)
	}

	var mod orderResponse
	f.do("PATCH", "/v1/instruments/PETR4/orders/1", `{"price":"34.10","quantity":"100"}`, 200, &mod)
	if mod.Status != "resting" || mod.Price != "34.10" || mod.RemainingQuantity != "100" {
		t.Fatalf("modify: %+v", mod)
	}

	var cxl orderResponse
	f.do("DELETE", "/v1/instruments/PETR4/orders/1", "", 200, &cxl)
	if cxl.Status != "canceled" || cxl.CanceledQuantity != "100" || cxl.Reason != "canceled_by_user" {
		t.Fatalf("cancel: %+v", cxl)
	}
	f.do("DELETE", "/v1/instruments/PETR4/orders/1", "", 404, &cxl)
	f.do("GET", "/v1/instruments/PETR4/orders/1", "", 404, nil)
}

func TestMarketIOCAndSelfTrade(t *testing.T) {
	f := newFixture(t)
	f.do("POST", "/v1/instruments/WINZ26/orders", `{"side":"sell","type":"limit","price":"128435","quantity":"2","account_id":7}`, 201, nil)
	f.do("POST", "/v1/instruments/WINZ26/orders", `{"side":"sell","type":"limit","price":"128440","quantity":"2","account_id":8}`, 201, nil)

	var mkt orderResponse
	f.do("POST", "/v1/instruments/WINZ26/orders",
		`{"side":"buy","type":"market","quantity":"5","account_id":7,"self_trade_prevention":"cancel_oldest"}`, 201, &mkt)
	if mkt.Status != "canceled" || mkt.Reason != "no_liquidity" || mkt.FilledQuantity != "2" ||
		mkt.CanceledQuantity != "3" || len(mkt.SelfTradeCanceled) != 1 || mkt.SelfTradeCanceled[0] != "1" {
		t.Fatalf("market: %+v", mkt)
	}
}

func TestRejections(t *testing.T) {
	f := newFixture(t)
	f.do("POST", "/v1/instruments/PETR4/orders", `{"side":"sell","type":"limit","price":"30","quantity":"100"}`, 201, nil)
	var r orderResponse
	f.do("POST", "/v1/instruments/PETR4/orders",
		`{"side":"buy","type":"limit","price":"30","quantity":"100","post_only":true}`, 422, &r)
	if r.Status != "rejected" || r.Reason != "post_only_would_cross" {
		t.Fatalf("post-only: %+v", r)
	}
	f.do("POST", "/v1/instruments/PETR4/orders",
		`{"side":"buy","type":"limit","price":"30","quantity":"500","time_in_force":"fok"}`, 422, &r)
	if r.Reason != "fill_or_kill_not_filled" {
		t.Fatalf("fok: %+v", r)
	}
}

func TestBadRequests(t *testing.T) {
	f := newFixture(t)
	for _, c := range []struct {
		name, method, path, body string
		status                   int
		msg                      string
	}{
		{"unknown instrument", "POST", "/v1/instruments/VALE3/orders", `{}`, 404, "unknown instrument"},
		{"no side", "POST", "/v1/instruments/PETR4/orders", `{"type":"limit","price":"1","quantity":"100"}`, 400, "side"},
		{"price off the tick", "POST", "/v1/instruments/PETR4/orders", `{"side":"buy","type":"limit","price":"34.155","quantity":"100"}`, 400, "not a multiple of the increment 0.01"},
		{"odd lot", "POST", "/v1/instruments/PETR4/orders", `{"side":"buy","type":"limit","price":"34","quantity":"150"}`, 400, "increment 100"},
		{"price as a number", "POST", "/v1/instruments/PETR4/orders", `{"side":"buy","type":"limit","price":34.1,"quantity":"100"}`, 400, "invalid JSON"},
		{"market with a price", "POST", "/v1/instruments/PETR4/orders", `{"side":"buy","type":"market","price":"34","quantity":"100"}`, 400, "no price"},
		{"limit without a price", "POST", "/v1/instruments/PETR4/orders", `{"side":"buy","type":"limit","quantity":"100"}`, 400, "need a price"},
		{"unknown field", "POST", "/v1/instruments/PETR4/orders", `{"side":"buy","type":"limit","price":"1","quantity":"100","leverage":10}`, 400, "unknown field"},
		{"two objects", "POST", "/v1/instruments/PETR4/orders", `{"side":"buy","type":"limit","price":"1","quantity":"100"}{}`, 400, "more than one"},
		{"huge body", "POST", "/v1/instruments/PETR4/orders", `{"side":"` + strings.Repeat("x", 5000) + `"}`, 400, "too large"},
		{"bad tif", "POST", "/v1/instruments/PETR4/orders", `{"side":"buy","type":"limit","price":"1","quantity":"100","time_in_force":"day"}`, 400, "time_in_force"},
		{"bad id", "DELETE", "/v1/instruments/PETR4/orders/abc", ``, 400, "positive integer"},
		{"zero id", "DELETE", "/v1/instruments/PETR4/orders/0", ``, 400, "positive integer"},
		{"bad depth", "GET", "/v1/instruments/PETR4/book?depth=0", ``, 400, "depth"},
	} {
		t.Run(c.name, func(t *testing.T) {
			var e map[string]string
			f.t = t
			f.do(c.method, c.path, c.body, c.status, &e)
			if !strings.Contains(e["error"], c.msg) {
				t.Errorf("error %q does not mention %q", e["error"], c.msg)
			}
		})
	}
}

func TestInstrumentsHealthAndMetrics(t *testing.T) {
	f := newFixture(t)
	var list []instrumentResponse
	f.do("GET", "/v1/instruments", "", 200, &list)
	if len(list) != 2 || list[0] != (instrumentResponse{"PETR4", "Petrobras PN", "0.01", "100"}) {
		t.Fatalf("instruments: %+v", list)
	}
	f.do("GET", "/healthz", "", 200, nil)
	f.do("GET", "/readyz", "", 200, nil)
	f.do("POST", "/v1/instruments/PETR4/orders", `{"side":"buy","type":"limit","price":"1","quantity":"100"}`, 201, nil)

	resp, err := http.Get(f.srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	for _, want := range []string{
		`http_requests_total{route="POST /v1/instruments/{symbol}/orders",code="201"} 1`,
		`matching_commands_total{instrument="PETR4",kind="submit"} 1`,
		`matching_resting_orders{instrument="PETR4"} 1`,
	} {
		if !strings.Contains(string(raw), want) {
			t.Errorf("metrics missing %s", want)
		}
	}

	f.eng.Close()
	f.do("GET", "/readyz", "", 503, nil)
	f.do("POST", "/v1/instruments/PETR4/orders", `{"side":"buy","type":"limit","price":"1","quantity":"100"}`, 503, nil)
}

type sse struct {
	event, data string
}

func readEvent(t *testing.T, r *bufio.Reader) sse {
	t.Helper()
	var ev sse
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			t.Fatalf("stream ended: %v", err)
		}
		line = strings.TrimSuffix(line, "\n")
		switch {
		case line == "" && ev.event != "":
			return ev
		case strings.HasPrefix(line, "event: "):
			ev.event = line[len("event: "):]
		case strings.HasPrefix(line, "data: "):
			ev.data = line[len("data: "):]
		}
	}
}

func TestStream(t *testing.T) {
	f := newFixture(t)
	f.do("POST", "/v1/instruments/PETR4/orders", `{"side":"sell","type":"limit","price":"34.15","quantity":"500"}`, 201, nil)

	resp, err := http.Get(f.srv.URL + "/v1/instruments/PETR4/stream")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("content type %q", ct)
	}
	r := bufio.NewReader(resp.Body)
	ev := readEvent(t, r)
	var snap snapshotEvent
	json.Unmarshal([]byte(ev.data), &snap)
	if ev.event != "snapshot" || len(snap.Asks) != 1 || snap.Asks[0].Quantity != "500" {
		t.Fatalf("first event %+v", ev)
	}

	f.do("POST", "/v1/instruments/PETR4/orders", `{"side":"buy","type":"limit","price":"34.15","quantity":"200"}`, 201, nil)
	ev = readEvent(t, r)
	var up updateEvent
	json.Unmarshal([]byte(ev.data), &up)
	if ev.event != "update" || up.Seq != snap.Seq+1 || len(up.Trades) != 1 || up.Trades[0].TakerSide != "buy" ||
		len(up.Levels) != 1 || up.Levels[0] != (levelChange{"sell", "34.15", "300"}) {
		t.Fatalf("update %+v", ev)
	}

	f.api.CloseStreams()
	if ev = readEvent(t, r); ev.event != "error" || !strings.Contains(ev.data, "shutting down") {
		t.Fatalf("on shutdown: %+v", ev)
	}
}
