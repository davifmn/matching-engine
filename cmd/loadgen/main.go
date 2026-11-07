// Command loadgen sends realistic order flow to matchd over HTTP and reports throughput
// and latency percentiles per operation.
//
//	loadgen -url http://localhost:8080 -symbol PETR4 -mid 34.00 -clients 16 -duration 30s
//
// Each client is its own account (so self-trade prevention is exercised) and runs its own
// seeded generator from package flow. It exits with status 1 if any request failed with a
// transport error or a 5xx.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/davifmn/matching-engine/internal/book"
	"github.com/davifmn/matching-engine/internal/flow"
	"github.com/davifmn/matching-engine/internal/instrument"
	"github.com/davifmn/matching-engine/internal/latency"
)

type config struct {
	url      string
	symbol   string
	mid      string
	clients  int
	duration time.Duration
	seed     uint64
	jsonOut  bool
}

func main() {
	var c config
	flag.StringVar(&c.url, "url", "http://localhost:8080", "matchd base URL")
	flag.StringVar(&c.symbol, "symbol", "PETR4", "instrument")
	flag.StringVar(&c.mid, "mid", "34.00", "starting mid price")
	flag.IntVar(&c.clients, "clients", 16, "concurrent clients")
	flag.DurationVar(&c.duration, "duration", 10*time.Second, "how long to run")
	flag.Uint64Var(&c.seed, "seed", 1, "random seed")
	flag.BoolVar(&c.jsonOut, "json", false, "print the report as JSON")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	rep, err := runLoad(ctx, c)
	if err != nil {
		fmt.Fprintln(os.Stderr, "loadgen:", err)
		os.Exit(1)
	}
	if c.jsonOut {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		enc.Encode(rep)
	} else {
		rep.print(os.Stdout)
	}
	if rep.Failures > 0 {
		os.Exit(1)
	}
}

type opStats struct {
	hist  latency.Histogram
	codes map[int]int
}

type opReport struct {
	Op     string         `json:"op"`
	Count  uint64         `json:"count"`
	P50us  float64        `json:"p50_us"`
	P90us  float64        `json:"p90_us"`
	P99us  float64        `json:"p99_us"`
	P999us float64        `json:"p999_us"`
	MaxUs  float64        `json:"max_us"`
	Codes  map[string]int `json:"status_codes"`
}

type report struct {
	Symbol     string     `json:"symbol"`
	Clients    int        `json:"clients"`
	Seconds    float64    `json:"seconds"`
	Requests   uint64     `json:"requests"`
	Throughput float64    `json:"requests_per_second"`
	Failures   int        `json:"failures"`
	Ops        []opReport `json:"ops"`
}

func runLoad(ctx context.Context, c config) (report, error) {
	client := &http.Client{
		Timeout:   10 * time.Second,
		Transport: &http.Transport{MaxIdleConnsPerHost: c.clients, MaxConnsPerHost: c.clients},
	}
	spec, err := fetchSpec(ctx, client, c.url, c.symbol)
	if err != nil {
		return report{}, err
	}
	mid, err := spec.Ticks(c.mid)
	if err != nil {
		return report{}, fmt.Errorf("-mid: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, c.duration)
	defer cancel()
	results := make([]map[flow.Kind]*opStats, c.clients)
	failures := make([]int, c.clients)
	var wg sync.WaitGroup
	begin := time.Now()
	for i := range c.clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w := &worker{
				client: client, spec: spec, base: c.url + "/v1/instruments/" + c.symbol + "/orders",
				account: uint64(i + 1), gen: flow.New(c.seed+uint64(i), mid, flow.DefaultMix),
				live: flow.NewLive(), orders: map[book.OrderID]liveOrder{}, stats: map[flow.Kind]*opStats{},
			}
			w.run(ctx)
			results[i], failures[i] = w.stats, w.failures
		}()
	}
	wg.Wait()
	elapsed := time.Since(begin)

	rep := report{Symbol: c.symbol, Clients: c.clients, Seconds: elapsed.Seconds()}
	var all latency.Histogram
	merged := map[flow.Kind]*opStats{}
	for i, r := range results {
		rep.Failures += failures[i]
		for k, s := range r {
			m := merged[k]
			if m == nil {
				m = &opStats{codes: map[int]int{}}
				merged[k] = m
			}
			m.hist.Merge(&s.hist)
			for code, n := range s.codes {
				m.codes[code] += n
			}
			all.Merge(&s.hist)
		}
	}
	rep.Requests = all.Count()
	rep.Throughput = float64(all.Count()) / elapsed.Seconds()
	kinds := []flow.Kind{flow.Passive, flow.Aggressive, flow.Market, flow.Cancel, flow.Modify}
	for _, k := range kinds {
		if m := merged[k]; m != nil {
			rep.Ops = append(rep.Ops, toReport(k.String(), &m.hist, m.codes))
		}
	}
	rep.Ops = append(rep.Ops, toReport("all", &all, nil))
	return rep, nil
}

func toReport(name string, h *latency.Histogram, codes map[int]int) opReport {
	us := func(d time.Duration) float64 { return float64(d.Nanoseconds()) / 1e3 }
	r := opReport{
		Op: name, Count: h.Count(), P50us: us(h.Quantile(0.5)), P90us: us(h.Quantile(0.9)),
		P99us: us(h.Quantile(0.99)), P999us: us(h.Quantile(0.999)), MaxUs: us(h.Max()),
	}
	if codes != nil {
		r.Codes = map[string]int{}
		for c, n := range codes {
			r.Codes[strconv.Itoa(c)] = n
		}
	}
	return r
}

func (r report) print(w io.Writer) {
	fmt.Fprintf(w, "%s: %d requests from %d clients in %.1fs = %.0f req/s, %d failures\n\n",
		r.Symbol, r.Requests, r.Clients, r.Seconds, r.Throughput, r.Failures)
	fmt.Fprintf(w, "%-11s %9s %9s %9s %9s %9s %9s  %s\n", "op", "count", "p50 µs", "p90 µs", "p99 µs", "p99.9 µs", "max µs", "status codes")
	for _, o := range r.Ops {
		codes := make([]string, 0, len(o.Codes))
		for c, n := range o.Codes {
			codes = append(codes, fmt.Sprintf("%s:%d", c, n))
		}
		sort.Strings(codes)
		fmt.Fprintf(w, "%-11s %9d %9.0f %9.0f %9.0f %9.0f %9.0f  %v\n", o.Op, o.Count, o.P50us, o.P90us, o.P99us, o.P999us, o.MaxUs, codes)
	}
}

func fetchSpec(ctx context.Context, client *http.Client, base, symbol string) (instrument.Spec, error) {
	req, _ := http.NewRequestWithContext(ctx, "GET", base+"/v1/instruments", nil)
	resp, err := client.Do(req)
	if err != nil {
		return instrument.Spec{}, err
	}
	defer resp.Body.Close()
	var specs []instrument.Spec
	if err := json.NewDecoder(resp.Body).Decode(&specs); err != nil {
		return instrument.Spec{}, fmt.Errorf("listing instruments: %w", err)
	}
	for _, s := range specs {
		if s.Symbol == symbol {
			return s, nil
		}
	}
	return instrument.Spec{}, fmt.Errorf("instrument %s not found on %s", symbol, base)
}

type liveOrder struct {
	side  book.Side
	price int64
	qty   int64
}

type worker struct {
	client   *http.Client
	spec     instrument.Spec
	base     string
	account  uint64
	gen      *flow.Generator
	live     *flow.Live
	orders   map[book.OrderID]liveOrder
	stats    map[flow.Kind]*opStats
	failures int
}

type response struct {
	OrderID           string `json:"order_id"`
	Status            string `json:"status"`
	RemainingQuantity string `json:"remaining_quantity"`
}

func (w *worker) run(ctx context.Context) {
	for ctx.Err() == nil {
		op := w.gen.Next(w.live.Len())
		var method, url string
		var body any
		var target book.OrderID
		switch op.Kind {
		case flow.Passive, flow.Aggressive:
			method, url = "POST", w.base
			body = map[string]any{
				"side": op.Side.String(), "type": "limit", "price": w.spec.Price(op.Price),
				"quantity": w.spec.Qty(op.Qty), "account_id": w.account,
			}
		case flow.Market:
			method, url = "POST", w.base
			body = map[string]any{"side": op.Side.String(), "type": "market", "quantity": w.spec.Qty(op.Qty), "account_id": w.account}
		case flow.Cancel:
			target = w.live.At(op.Pick)
			method, url = "DELETE", w.base+"/"+strconv.FormatUint(uint64(target), 10)
		case flow.Modify:
			target = w.live.At(op.Pick)
			o := w.orders[target]
			price, qty := o.price, max(o.qty/2, 1)
			if op.Reprice {
				qty = o.qty
				if o.side == book.Buy {
					price = max(price-1, 1)
				} else {
					price++
				}
			}
			method, url = "PATCH", w.base+"/"+strconv.FormatUint(uint64(target), 10)
			body = map[string]string{"price": w.spec.Price(price), "quantity": w.spec.Qty(qty)}
		}
		start := time.Now()
		code, resp, err := w.send(ctx, method, url, body)
		took := time.Since(start)
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return // the run is over; this request does not count
		}
		s := w.stats[op.Kind]
		if s == nil {
			s = &opStats{codes: map[int]int{}}
			w.stats[op.Kind] = s
		}
		s.hist.Record(took)
		s.codes[code]++
		if err != nil || code >= 500 {
			w.failures++
			continue
		}
		w.track(op, target, resp, body)
	}
}

// track keeps the list of this client's resting orders up to date from responses. Orders
// filled by other clients are only discovered when a cancel or modify gets a 404, which is
// how a real client finds out too.
func (w *worker) track(op flow.Op, target book.OrderID, resp response, body any) {
	if target != 0 && (resp.Status != "resting") {
		w.live.Remove(target)
		delete(w.orders, target)
		return
	}
	if resp.Status != "resting" {
		return
	}
	id, err := strconv.ParseUint(resp.OrderID, 10, 64)
	if err != nil {
		return
	}
	lots, _ := w.spec.Lots(resp.RemainingQuantity)
	o := w.orders[book.OrderID(id)]
	switch op.Kind {
	case flow.Modify:
		m := body.(map[string]string)
		o.price, _ = w.spec.Ticks(m["price"])
	default:
		o = liveOrder{side: op.Side, price: op.Price}
	}
	o.qty = lots
	w.orders[book.OrderID(id)] = o
	w.live.Add(book.OrderID(id))
}

func (w *worker) send(ctx context.Context, method, url string, body any) (int, response, error) {
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, rd)
	if err != nil {
		return 0, response{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := w.client.Do(req)
	if err != nil {
		return 0, response{}, err
	}
	defer resp.Body.Close()
	var r response
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, r, err
	}
	_ = json.Unmarshal(raw, &r)
	return resp.StatusCode, r, nil
}
