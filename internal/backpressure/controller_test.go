package backpressure

import (
	"testing"
	"time"

	"e-go04/internal/breaker"
)

func TestController_WatermarkTransitions(t *testing.T) {
	cfg := ControllerConfig{
		HighWatermark:    100,
		LowWatermark:     20,
		MinThrottleDelay: 10 * time.Millisecond,
		MaxThrottleDelay: 50 * time.Millisecond,
	}
	ctrl := NewController(cfg)

	// Below high watermark, breaker CLOSED -> not throttled
	if ctrl.Evaluate(50, breaker.StateClosed) {
		t.Fatalf("expected not throttled at depth 50")
	}
	if ctrl.IsThrottled() {
		t.Fatalf("expected IsThrottled false")
	}
	if delay := ctrl.ThrottleDelay(50); delay != 0 {
		t.Fatalf("expected 0 delay when not throttled, got %v", delay)
	}

	// Reach high watermark -> triggers throttle
	if !ctrl.Evaluate(100, breaker.StateClosed) {
		t.Fatalf("expected throttled at depth 100")
	}
	if !ctrl.IsThrottled() {
		t.Fatalf("expected IsThrottled true")
	}

	// Buffer drops to 60 (between low and high) -> hysteresis keeps throttling active
	if !ctrl.Evaluate(60, breaker.StateClosed) {
		t.Fatalf("expected still throttled at depth 60 due to hysteresis")
	}

	// Buffer drops to 15 (below low watermark) -> releases throttle
	if ctrl.Evaluate(15, breaker.StateClosed) {
		t.Fatalf("expected throttling released at depth 15")
	}
	if ctrl.IsThrottled() {
		t.Fatalf("expected IsThrottled false after release")
	}
}

func TestController_BreakerOpenForcesThrottle(t *testing.T) {
	cfg := ControllerConfig{
		HighWatermark: 100,
		LowWatermark:  20,
	}
	ctrl := NewController(cfg)

	// Even with 0 buffer, if breaker is OPEN, throttling must activate
	if !ctrl.Evaluate(0, breaker.StateOpen) {
		t.Fatalf("expected throttle when breaker is OPEN")
	}

	// Once breaker transitions to HALF_OPEN and buffer is 0, throttling can release
	if ctrl.Evaluate(0, breaker.StateHalfOpen) {
		t.Fatalf("expected throttle released when breaker in HALF_OPEN and buffer empty")
	}
}

func TestController_DynamicCalculations(t *testing.T) {
	cfg := ControllerConfig{
		HighWatermark:    100,
		LowWatermark:     20,
		MinThrottleDelay: 10 * time.Millisecond,
		MaxThrottleDelay: 50 * time.Millisecond,
	}
	ctrl := NewController(cfg)

	// Not throttled: full batch, 0 delay
	if b := ctrl.DynamicBatchSize(1000, 10); b != 1000 {
		t.Fatalf("expected 1000, got %d", b)
	}

	// Activate throttle
	ctrl.Evaluate(100, breaker.StateClosed)

	reducedBatch := ctrl.DynamicBatchSize(1000, 100)
	if reducedBatch >= 1000 || reducedBatch <= 0 {
		t.Fatalf("expected reduced batch, got %d", reducedBatch)
	}

	delay := ctrl.ThrottleDelay(100)
	if delay < cfg.MinThrottleDelay || delay > cfg.MaxThrottleDelay {
		t.Fatalf("expected delay in range [%v, %v], got %v", cfg.MinThrottleDelay, cfg.MaxThrottleDelay, delay)
	}
}
