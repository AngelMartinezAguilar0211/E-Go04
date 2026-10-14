package metrics

import (
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// LatencyStats summarizes measured end-to-end latency percentiles.
type LatencyStats struct {
	Count int64         `json:"count"`
	Min   time.Duration `json:"min"`
	Mean  time.Duration `json:"mean"`
	P50   time.Duration `json:"p50"`
	P90   time.Duration `json:"p90"`
	P95   time.Duration `json:"p95"`
	P99   time.Duration `json:"p99"`
	Max   time.Duration `json:"max"`
}

// LatencyTracker records end-to-end latency measurements using reservoir sampling.
type LatencyTracker struct {
	mu         sync.Mutex
	sampleSize int
	samples    []time.Duration
	totalCount int64
	minNanos   int64
	maxNanos   int64
	sumNanos   int64
	rng        *rand.Rand
}

// NewLatencyTracker creates a new LatencyTracker with the specified reservoir capacity.
func NewLatencyTracker(sampleSize int) *LatencyTracker {
	if sampleSize <= 0 {
		sampleSize = 100000
	}
	return &LatencyTracker{
		sampleSize: sampleSize,
		samples:    make([]time.Duration, 0, sampleSize),
		minNanos:   -1,
		rng:        rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Record records an observed latency duration.
func (lt *LatencyTracker) Record(d time.Duration) {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	nanos := d.Nanoseconds()
	if nanos < 0 {
		nanos = 0
		d = 0
	}

	lt.totalCount++
	lt.sumNanos += nanos

	if lt.minNanos < 0 || nanos < lt.minNanos {
		lt.minNanos = nanos
	}
	if nanos > lt.maxNanos {
		lt.maxNanos = nanos
	}

	// Reservoir sampling (Algorithm R)
	if len(lt.samples) < lt.sampleSize {
		lt.samples = append(lt.samples, d)
	} else {
		idx := lt.rng.Int63n(lt.totalCount)
		if idx < int64(lt.sampleSize) {
			lt.samples[idx] = d
		}
	}
}

// Stats computes summary statistics and percentiles from the sampled latencies.
func (lt *LatencyTracker) Stats() LatencyStats {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	if lt.totalCount == 0 || len(lt.samples) == 0 {
		return LatencyStats{}
	}

	// Sort a copy of the sampled latencies
	sorted := make([]time.Duration, len(lt.samples))
	copy(sorted, lt.samples)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i] < sorted[j]
	})

	n := len(sorted)
	p50 := sorted[int(float64(n)*0.50)]
	p90 := sorted[int(float64(n)*0.90)]
	p95 := sorted[int(float64(n)*0.95)]
	p99 := sorted[int(float64(n)*0.99)]

	meanNanos := lt.sumNanos / lt.totalCount

	return LatencyStats{
		Count: lt.totalCount,
		Min:   time.Duration(lt.minNanos),
		Mean:  time.Duration(meanNanos),
		P50:   p50,
		P90:   p90,
		P95:   p95,
		P99:   p99,
		Max:   time.Duration(lt.maxNanos),
	}
}

// PrintTable outputs the latency percentiles in a formatted table.
func (ls LatencyStats) PrintTable() {
	fmt.Printf("\nDistribución de Latencia End-to-End (Publicación -> Procesamiento):\n")
	fmt.Printf("  • Muestras analizadas : %10d\n", ls.Count)
	fmt.Printf("  • Mínima              : %10v\n", ls.Min.Round(time.Microsecond))
	fmt.Printf("  • Media               : %10v\n", ls.Mean.Round(time.Microsecond))
	fmt.Printf("  • Mediana (p50)       : %10v\n", ls.P50.Round(time.Microsecond))
	fmt.Printf("  • Percentil 90 (p90)  : %10v\n", ls.P90.Round(time.Microsecond))
	fmt.Printf("  • Percentil 95 (p95)  : %10v\n", ls.P95.Round(time.Microsecond))
	fmt.Printf("  • Percentil 99 (p99)  : %10v\n", ls.P99.Round(time.Microsecond))
	fmt.Printf("  • Máxima              : %10v\n", ls.Max.Round(time.Microsecond))
}

// ThroughputMeter tracks ingestion and processing rate in events per second.
type ThroughputMeter struct {
	count     int64
	startTime time.Time
}

// NewThroughputMeter initializes a ThroughputMeter.
func NewThroughputMeter() *ThroughputMeter {
	return &ThroughputMeter{
		startTime: time.Now(),
	}
}

// Add records N processed events.
func (tm *ThroughputMeter) Add(n int64) {
	atomic.AddInt64(&tm.count, n)
}

// TotalEvents returns the cumulative event count.
func (tm *ThroughputMeter) TotalEvents() int64 {
	return atomic.LoadInt64(&tm.count)
}

// EventsPerSecond calculates the cumulative average throughput.
func (tm *ThroughputMeter) EventsPerSecond() float64 {
	elapsed := time.Since(tm.startTime).Seconds()
	if elapsed <= 0 {
		return 0
	}
	return float64(atomic.LoadInt64(&tm.count)) / elapsed
}

// Elapsed returns the total elapsed time since the meter was created.
func (tm *ThroughputMeter) Elapsed() time.Duration {
	return time.Since(tm.startTime)
}
