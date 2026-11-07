package metrics

import (
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestTextFormat(t *testing.T) {
	r := NewRegistry()
	orders := r.Counter("orders_total", "Orders received.", "instrument", "kind")
	orders.With("PETR4", "submit").Add(3)
	orders.With("PETR4", "cancel").Inc()
	r.Gauge("resting_orders", "Orders on the book.", "instrument").With(`we"ird\`).Set(2.5)
	h := r.Histogram("latency_seconds", "Latency.", []float64{0.001, 0.01}, "instrument")
	h.With("PETR4").Observe(0.0009765625) // powers of two, so the sum is exact
	h.With("PETR4").ObserveDuration(3906250 * time.Nanosecond)
	h.With("PETR4").Observe(1)
	r.GaugeFunc("up", "Always 1.", func() float64 { return 1 })

	rec := httptest.NewRecorder()
	r.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	got := rec.Body.String()
	want := `# HELP latency_seconds Latency.
# TYPE latency_seconds histogram
latency_seconds_bucket{instrument="PETR4",le="0.001"} 1
latency_seconds_bucket{instrument="PETR4",le="0.01"} 2
latency_seconds_bucket{instrument="PETR4",le="+Inf"} 3
latency_seconds_sum{instrument="PETR4"} 1.0048828125
latency_seconds_count{instrument="PETR4"} 3
# HELP orders_total Orders received.
# TYPE orders_total counter
orders_total{instrument="PETR4",kind="cancel"} 1
orders_total{instrument="PETR4",kind="submit"} 3
# HELP resting_orders Orders on the book.
# TYPE resting_orders gauge
resting_orders{instrument="we\"ird\\"} 2.5
# HELP up Always 1.
# TYPE up gauge
up 1
`
	if got != want {
		t.Errorf("got:\n%s\nwant:\n%s", got, want)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain; version=0.0.4") {
		t.Errorf("content type %q", ct)
	}
}

func TestConcurrentUpdates(t *testing.T) {
	r := NewRegistry()
	c := r.Counter("c", "c", "w")
	g := r.Gauge("g", "g")
	h := r.Histogram("h", "h", LatencyBuckets())
	var wg sync.WaitGroup
	for w := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 1000 {
				c.With(string(rune('a' + w%2))).Inc()
				g.With().Add(1)
				h.With().Observe(0.001)
			}
		}()
	}
	wg.Wait()
	if a, b := c.With("a").Value(), c.With("b").Value(); a+b != 8000 {
		t.Errorf("counter total %d", a+b)
	}
	if g.With().Value() != 8000 || h.With().Count() != 8000 {
		t.Errorf("gauge %v, histogram %d", g.With().Value(), h.With().Count())
	}
}

func TestMisuse(t *testing.T) {
	r := NewRegistry()
	r.Counter("x", "x", "a")
	mustPanic(t, "duplicate", func() { r.Counter("x", "x") })
	mustPanic(t, "label count", func() { r.Gauge("y", "y", "a", "b").With("only one") })
	mustPanic(t, "unsorted buckets", func() { r.Histogram("z", "z", []float64{2, 1}) })
}

func mustPanic(t *testing.T, name string, fn func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Errorf("%s: no panic", name)
		}
	}()
	fn()
}
