package metrics

import (
	"testing"
	"time"
)

func TestThroughputMeter(t *testing.T) {
	tm := NewThroughputMeter()
	tm.Add(100)
	tm.Add(200)

	if tm.TotalEvents() != 300 {
		t.Fatalf("expected 300 total events, got %d", tm.TotalEvents())
	}

	time.Sleep(10 * time.Millisecond)
	rate := tm.EventsPerSecond()
	if rate <= 0 {
		t.Fatalf("expected positive throughput rate, got %f", rate)
	}
}

func TestLatencyTracker_Percentiles(t *testing.T) {
	tracker := NewLatencyTracker(1000)

	for i := 1; i <= 100; i++ {
		tracker.Record(time.Duration(i) * time.Millisecond)
	}

	stats := tracker.Stats()
	if stats.Count != 100 {
		t.Fatalf("expected 100 samples, got %d", stats.Count)
	}
	if stats.Min != 1*time.Millisecond {
		t.Fatalf("expected min 1ms, got %v", stats.Min)
	}
	if stats.Max != 100*time.Millisecond {
		t.Fatalf("expected max 100ms, got %v", stats.Max)
	}
	if stats.P50 < 45*time.Millisecond || stats.P50 > 55*time.Millisecond {
		t.Fatalf("expected p50 around 50ms, got %v", stats.P50)
	}
	if stats.P95 < 90*time.Millisecond || stats.P95 > 98*time.Millisecond {
		t.Fatalf("expected p95 around 95ms, got %v", stats.P95)
	}
}
