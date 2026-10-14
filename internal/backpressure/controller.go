package backpressure

import (
	"fmt"
	"sync"
	"time"

	"e-go04/internal/breaker"
)

// ControllerConfig defines thresholds for backpressure triggers and recovery.
type ControllerConfig struct {
	// HighWatermark is the buffer depth at which ingestion throttling activates.
	HighWatermark int
	// LowWatermark is the buffer depth at which normal ingestion resumes.
	LowWatermark int
	// MinThrottleDelay is the minimum pause duration applied when throttled.
	MinThrottleDelay time.Duration
	// MaxThrottleDelay is the maximum pause duration applied under high saturation.
	MaxThrottleDelay time.Duration
}

// DefaultControllerConfig provides balanced thresholds for streaming ingestion.
func DefaultControllerConfig() ControllerConfig {
	return ControllerConfig{
		HighWatermark:    500,
		LowWatermark:     50,
		MinThrottleDelay: 50 * time.Millisecond,
		MaxThrottleDelay: 300 * time.Millisecond,
	}
}

// ControllerStats contains runtime telemetry for backpressure activations.
type ControllerStats struct {
	IsThrottled           bool          `json:"is_throttled"`
	ThrottleCount         int64         `json:"throttle_count"`
	TotalThrottleDuration time.Duration `json:"total_throttle_duration"`
}

// BackpressureController monitors buffer depth and circuit breaker status to modulate ingestion rate.
type BackpressureController struct {
	mu                    sync.Mutex
	config                ControllerConfig
	isThrottled           bool
	throttledAt           time.Time
	throttleCount         int64
	totalThrottleDuration time.Duration
}

// NewController creates a BackpressureController with given thresholds.
func NewController(cfg ControllerConfig) *BackpressureController {
	if cfg.HighWatermark <= 0 {
		cfg.HighWatermark = 500
	}
	if cfg.LowWatermark <= 0 || cfg.LowWatermark >= cfg.HighWatermark {
		cfg.LowWatermark = cfg.HighWatermark / 10
	}
	if cfg.MinThrottleDelay <= 0 {
		cfg.MinThrottleDelay = 50 * time.Millisecond
	}
	if cfg.MaxThrottleDelay < cfg.MinThrottleDelay {
		cfg.MaxThrottleDelay = cfg.MinThrottleDelay * 4
	}

	return &BackpressureController{
		config: cfg,
	}
}

// Evaluate updates backpressure state based on current buffer depth and circuit breaker state.
func (c *BackpressureController) Evaluate(bufferDepth int, breakerState breaker.State) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	shouldThrottle := false

	// Breaker is open: downstream external service is failing
	if breakerState == breaker.StateOpen {
		shouldThrottle = true
	} else if bufferDepth >= c.config.HighWatermark {
		// Buffer capacity exceeded high watermark
		shouldThrottle = true
	} else if c.isThrottled && bufferDepth > c.config.LowWatermark {
		// Hysteresis: retain throttle until buffer drops below low watermark
		shouldThrottle = true
	}

	if shouldThrottle && !c.isThrottled {
		// Transition into throttled state
		c.isThrottled = true
		c.throttledAt = now
		c.throttleCount++
	} else if !shouldThrottle && c.isThrottled {
		// Recover to normal state
		c.isThrottled = false
		c.totalThrottleDuration += now.Sub(c.throttledAt)
	}

	return c.isThrottled
}

// IsThrottled returns true if the system is currently applying backpressure.
func (c *BackpressureController) IsThrottled() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.isThrottled
}

// ThrottleDelay computes dynamic delay duration to throttle consumer reads.
func (c *BackpressureController) ThrottleDelay(bufferDepth int) time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.isThrottled {
		return 0
	}

	if bufferDepth <= c.config.LowWatermark {
		return c.config.MinThrottleDelay
	}

	// Linear interpolation between Min and Max delay based on buffer depth
	rangeSpan := c.config.HighWatermark - c.config.LowWatermark
	if rangeSpan <= 0 {
		return c.config.MaxThrottleDelay
	}

	excess := bufferDepth - c.config.LowWatermark
	factor := float64(excess) / float64(rangeSpan)
	if factor > 1.0 {
		factor = 1.0
	}

	delayRange := float64(c.config.MaxThrottleDelay - c.config.MinThrottleDelay)
	return c.config.MinThrottleDelay + time.Duration(factor*delayRange)
}

// DynamicBatchSize calculates the recommended fetch size for XREADGROUP under load.
func (c *BackpressureController) DynamicBatchSize(defaultBatch int64, bufferDepth int) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.isThrottled {
		return defaultBatch
	}

	// Under heavy backpressure, reduce batch size to avoid burst buffer inflation
	reduced := defaultBatch / 5
	if reduced < 10 {
		reduced = 10
	}
	return reduced
}

// Stats returns a snapshot of backpressure telemetry.
func (c *BackpressureController) Stats() ControllerStats {
	c.mu.Lock()
	defer c.mu.Unlock()

	totalDuration := c.totalThrottleDuration
	if c.isThrottled {
		totalDuration += time.Since(c.throttledAt)
	}

	return ControllerStats{
		IsThrottled:           c.isThrottled,
		ThrottleCount:         c.throttleCount,
		TotalThrottleDuration: totalDuration,
	}
}

// PrintReport outputs summary metrics for the backpressure controller.
func (c *BackpressureController) PrintReport() {
	s := c.Stats()
	fmt.Printf("Backpressure Telemetry (internal/backpressure):\n")
	fmt.Printf("  • Throttling State           : %t\n", s.IsThrottled)
	fmt.Printf("  • Total Throttling Activations: %d\n", s.ThrottleCount)
	fmt.Printf("  • Total Throttling Duration   : %s\n", s.TotalThrottleDuration.Round(time.Millisecond))
	fmt.Printf("  • High Watermark Threshold   : %d items\n", c.config.HighWatermark)
	fmt.Printf("  • Low Watermark Threshold    : %d items\n", c.config.LowWatermark)
}
