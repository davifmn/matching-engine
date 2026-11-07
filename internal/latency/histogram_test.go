package latency

import (
	"math/rand/v2"
	"slices"
	"testing"
	"time"
)

func TestBucketsCoverEveryValue(t *testing.T) {
	for _, v := range []int64{0, 1, 127, 128, 129, 255, 256, 1_000, 123_456_789, 1 << 40, 1<<62 + 12345} {
		i := index(v)
		if lo := lowest(i); lo > v {
			t.Errorf("value %d in bucket %d whose lowest value is %d", v, i, lo)
		}
		if i+1 < buckets && lowest(i+1) <= v {
			t.Errorf("value %d should be in bucket %d or later", v, i+1)
		}
	}
}

func TestQuantilesWithinOnePointSixPercent(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 1))
	var h Histogram
	vals := make([]int64, 100_000)
	for i := range vals {
		vals[i] = int64(rng.ExpFloat64() * 50_000) // ~50µs mean, long tail
		h.Record(time.Duration(vals[i]))
	}
	slices.Sort(vals)
	for _, q := range []float64{0.5, 0.9, 0.99, 0.999} {
		exact := float64(vals[int(q*float64(len(vals)))-1])
		got := float64(h.Quantile(q))
		if got > exact*1.0001 || got < exact*(1-1.0/64)-1 {
			t.Errorf("q%.3f = %.0f, exact %.0f", q, got, exact)
		}
	}
	if h.Count() != uint64(len(vals)) || h.Max() != time.Duration(vals[len(vals)-1]) {
		t.Errorf("count=%d max=%v", h.Count(), h.Max())
	}
}

func TestMerge(t *testing.T) {
	var a, b Histogram
	a.Record(10 * time.Microsecond)
	b.Record(time.Millisecond)
	b.Record(time.Microsecond)
	a.Merge(&b)
	if a.Count() != 3 || a.Max() != time.Millisecond || a.Quantile(0) != time.Microsecond {
		t.Errorf("merged: %s", a.Summary())
	}
	var empty Histogram
	if empty.Quantile(0.99) != 0 || empty.Mean() != 0 {
		t.Error("empty histogram should report zeros")
	}
}
