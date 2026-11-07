// Package metrics exposes counters, gauges and histograms in the Prometheus text format.
//
// It is a small stand-in for the official client library, so the service has no
// third-party dependencies (see the README). Updates are lock-free atomics; the
// registry lock is only taken to create a labeled series and to render /metrics.
package metrics

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type kind uint8

const (
	kindCounter kind = iota
	kindGauge
	kindHistogram
)

var typeNames = [...]string{"counter", "gauge", "histogram"}

// Registry holds metric families.
type Registry struct {
	mu       sync.Mutex
	families map[string]*family
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry { return &Registry{families: map[string]*family{}} }

type family struct {
	name, help string
	kind       kind
	labels     []string
	buckets    []float64
	fn         func() float64 // gauge funcs

	mu     sync.RWMutex
	series map[string]*series
}

type series struct {
	values []string
	c      Counter
	g      Gauge
	h      *Histogram
}

func (r *Registry) register(name, help string, k kind, labels []string, buckets []float64, fn func() float64) *family {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, dup := r.families[name]; dup {
		panic("metrics: " + name + " registered twice")
	}
	f := &family{name: name, help: help, kind: k, labels: labels, buckets: buckets, fn: fn, series: map[string]*series{}}
	r.families[name] = f
	return f
}

func (f *family) with(values []string) *series {
	if len(values) != len(f.labels) {
		panic(fmt.Sprintf("metrics: %s takes labels %v, got %d values", f.name, f.labels, len(values)))
	}
	key := strings.Join(values, "\xff")
	f.mu.RLock()
	s := f.series[key]
	f.mu.RUnlock()
	if s != nil {
		return s
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if s = f.series[key]; s == nil {
		s = &series{values: slices.Clone(values)}
		if f.kind == kindHistogram {
			s.h = newHistogram(f.buckets)
		}
		f.series[key] = s
	}
	return s
}

// Counter only goes up.
type Counter struct{ v atomic.Uint64 }

func (c *Counter) Inc()          { c.v.Add(1) }
func (c *Counter) Add(n uint64)  { c.v.Add(n) }
func (c *Counter) Value() uint64 { return c.v.Load() }

// Gauge goes up and down.
type Gauge struct{ bits atomic.Uint64 }

func (g *Gauge) Set(v float64) { g.bits.Store(math.Float64bits(v)) }
func (g *Gauge) Add(d float64) {
	for {
		old := g.bits.Load()
		if g.bits.CompareAndSwap(old, math.Float64bits(math.Float64frombits(old)+d)) {
			return
		}
	}
}
func (g *Gauge) Value() float64 { return math.Float64frombits(g.bits.Load()) }

// Histogram counts observations in cumulative buckets.
type Histogram struct {
	upper   []float64
	counts  []atomic.Uint64 // one per bucket, plus +Inf
	count   atomic.Uint64
	sumBits atomic.Uint64
}

func newHistogram(upper []float64) *Histogram {
	return &Histogram{upper: upper, counts: make([]atomic.Uint64, len(upper)+1)}
}

// Observe records one value.
func (h *Histogram) Observe(v float64) {
	i, _ := slices.BinarySearch(h.upper, v)
	h.counts[i].Add(1)
	h.count.Add(1)
	for {
		old := h.sumBits.Load()
		if h.sumBits.CompareAndSwap(old, math.Float64bits(math.Float64frombits(old)+v)) {
			return
		}
	}
}

// ObserveDuration records a duration in seconds.
func (h *Histogram) ObserveDuration(d time.Duration) { h.Observe(d.Seconds()) }

// Count returns the number of observations.
func (h *Histogram) Count() uint64 { return h.count.Load() }

// CounterVec is a counter family with labels.
type CounterVec struct{ f *family }

// GaugeVec is a gauge family with labels.
type GaugeVec struct{ f *family }

// HistogramVec is a histogram family with labels.
type HistogramVec struct{ f *family }

func (r *Registry) Counter(name, help string, labels ...string) CounterVec {
	return CounterVec{r.register(name, help, kindCounter, labels, nil, nil)}
}

func (r *Registry) Gauge(name, help string, labels ...string) GaugeVec {
	return GaugeVec{r.register(name, help, kindGauge, labels, nil, nil)}
}

// Histogram registers a histogram family; buckets are sorted upper bounds.
func (r *Registry) Histogram(name, help string, buckets []float64, labels ...string) HistogramVec {
	if !slices.IsSorted(buckets) {
		panic("metrics: buckets of " + name + " are not sorted")
	}
	return HistogramVec{r.register(name, help, kindHistogram, labels, buckets, nil)}
}

// GaugeFunc registers an unlabeled gauge whose value is read at scrape time.
func (r *Registry) GaugeFunc(name, help string, fn func() float64) {
	r.register(name, help, kindGauge, nil, nil, fn)
}

func (v CounterVec) With(values ...string) *Counter     { return &v.f.with(values).c }
func (v GaugeVec) With(values ...string) *Gauge         { return &v.f.with(values).g }
func (v HistogramVec) With(values ...string) *Histogram { return v.f.with(values).h }

// LatencyBuckets go from 1µs to about 2s in powers of two, which suits both the matching
// path (microseconds) and HTTP and fsync (milliseconds).
func LatencyBuckets() []float64 {
	b := make([]float64, 22)
	for i := range b {
		b[i] = 1e-6 * math.Pow(2, float64(i))
	}
	return b
}

// SizeBuckets are powers of two from 1 to 1024.
func SizeBuckets() []float64 {
	b := make([]float64, 11)
	for i := range b {
		b[i] = math.Pow(2, float64(i))
	}
	return b
}

// Write renders every family in the Prometheus text format, sorted by name.
func (r *Registry) Write(w io.Writer) error {
	r.mu.Lock()
	fams := make([]*family, 0, len(r.families))
	for _, f := range r.families {
		fams = append(fams, f)
	}
	r.mu.Unlock()
	slices.SortFunc(fams, func(a, b *family) int { return strings.Compare(a.name, b.name) })

	bw := bufio.NewWriter(w)
	for _, f := range fams {
		fmt.Fprintf(bw, "# HELP %s %s\n# TYPE %s %s\n", f.name, escapeHelp(f.help), f.name, typeNames[f.kind])
		if f.fn != nil {
			fmt.Fprintf(bw, "%s %s\n", f.name, formatFloat(f.fn()))
			continue
		}
		f.mu.RLock()
		ss := make([]*series, 0, len(f.series))
		for _, s := range f.series {
			ss = append(ss, s)
		}
		f.mu.RUnlock()
		slices.SortFunc(ss, func(a, b *series) int { return slices.Compare(a.values, b.values) })
		for _, s := range ss {
			labels := formatLabels(f.labels, s.values)
			switch f.kind {
			case kindCounter:
				fmt.Fprintf(bw, "%s%s %d\n", f.name, labels, s.c.Value())
			case kindGauge:
				fmt.Fprintf(bw, "%s%s %s\n", f.name, labels, formatFloat(s.g.Value()))
			case kindHistogram:
				writeHistogram(bw, f, s)
			}
		}
	}
	return bw.Flush()
}

func writeHistogram(w io.Writer, f *family, s *series) {
	var cum uint64
	for i := range s.h.counts {
		cum += s.h.counts[i].Load()
		le := "+Inf"
		if i < len(s.h.upper) {
			le = formatFloat(s.h.upper[i])
		}
		fmt.Fprintf(w, "%s_bucket%s %d\n", f.name, formatLabels(append(slices.Clone(f.labels), "le"), append(slices.Clone(s.values), le)), cum)
	}
	labels := formatLabels(f.labels, s.values)
	fmt.Fprintf(w, "%s_sum%s %s\n", f.name, labels, formatFloat(math.Float64frombits(s.h.sumBits.Load())))
	fmt.Fprintf(w, "%s_count%s %d\n", f.name, labels, s.h.count.Load())
}

func formatLabels(names, values []string) string {
	if len(names) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteByte('{')
	for i, n := range names {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(n)
		b.WriteString(`="`)
		b.WriteString(escapeLabel(values[i]))
		b.WriteByte('"')
	}
	b.WriteByte('}')
	return b.String()
}

var (
	labelEscaper = strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)
	helpEscaper  = strings.NewReplacer(`\`, `\\`, "\n", `\n`)
)

func escapeLabel(s string) string { return labelEscaper.Replace(s) }
func escapeHelp(s string) string  { return helpEscaper.Replace(s) }

func formatFloat(v float64) string {
	switch {
	case math.IsInf(v, 1):
		return "+Inf"
	case math.IsInf(v, -1):
		return "-Inf"
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}

// Handler serves the registry.
func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_ = r.Write(w)
	})
}
