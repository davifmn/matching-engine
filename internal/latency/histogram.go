// Package latency is a fixed-size log-linear histogram for latency percentiles: O(1)
// recording with no allocation and a relative error under 1.6%, in the spirit of
// HdrHistogram. The book benchmarks and the load generator report p50/p99/p99.9 with it.
package latency

import (
	"fmt"
	"math/bits"
	"time"
)

const (
	subBits    = 6
	subBuckets = 1 << subBits // 64 sub-buckets per power of two
	linear     = 2 * subBuckets
	buckets    = linear + (64-subBits-1)*subBuckets
)

// Histogram counts durations. The zero value is ready to use. Not safe for concurrent
// use: give each goroutine its own and Merge them.
type Histogram struct {
	counts [buckets]uint64
	n      uint64
	min    int64
	max    int64
	sum    float64
}

func index(v int64) int {
	if v < linear {
		return int(max(v, 0))
	}
	shift := bits.Len64(uint64(v)) - subBits - 1 // v>>shift is in [64, 128)
	return linear + (shift-1)*subBuckets + int(uint64(v)>>shift) - subBuckets
}

// lowest returns the smallest value that falls into bucket i.
func lowest(i int) int64 {
	if i < linear {
		return int64(i)
	}
	i -= linear
	shift := i/subBuckets + 1
	return int64(subBuckets+i%subBuckets) << shift
}

// Record adds one duration.
func (h *Histogram) Record(d time.Duration) {
	v := int64(d)
	h.counts[index(v)]++
	if h.n == 0 || v < h.min {
		h.min = v
	}
	if v > h.max {
		h.max = v
	}
	h.n++
	h.sum += float64(v)
}

// Merge adds the counts of o into h.
func (h *Histogram) Merge(o *Histogram) {
	if o.n == 0 {
		return
	}
	for i, c := range o.counts {
		h.counts[i] += c
	}
	if h.n == 0 || o.min < h.min {
		h.min = o.min
	}
	h.max = max(h.max, o.max)
	h.n += o.n
	h.sum += o.sum
}

// Count returns the number of recorded durations.
func (h *Histogram) Count() uint64 { return h.n }

// Mean returns the average duration.
func (h *Histogram) Mean() time.Duration {
	if h.n == 0 {
		return 0
	}
	return time.Duration(h.sum / float64(h.n))
}

// Max returns the largest duration recorded.
func (h *Histogram) Max() time.Duration { return time.Duration(h.max) }

// Quantile returns the duration below which a fraction q (0..1) of the samples fall,
// rounded down to its bucket.
func (h *Histogram) Quantile(q float64) time.Duration {
	if h.n == 0 {
		return 0
	}
	rank := uint64(q*float64(h.n) + 0.5)
	rank = min(max(rank, 1), h.n)
	var seen uint64
	for i, c := range h.counts {
		seen += c
		if seen >= rank {
			return time.Duration(min(max(lowest(i), h.min), h.max))
		}
	}
	return time.Duration(h.max)
}

// Summary formats count, mean and the usual percentiles.
func (h *Histogram) Summary() string {
	return fmt.Sprintf("n=%d mean=%v p50=%v p90=%v p99=%v p99.9=%v max=%v",
		h.n, h.Mean(), h.Quantile(0.5), h.Quantile(0.9), h.Quantile(0.99), h.Quantile(0.999), h.Max())
}
