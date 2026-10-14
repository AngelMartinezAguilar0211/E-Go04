package breaker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// ErrCircuitOpen is returned when execution is rejected because the breaker is in OPEN state.
var ErrCircuitOpen = errors.New("circuit breaker is OPEN: fast-failing request")

// State represents the operating mode of the circuit breaker.
type State string

const (
	StateClosed   State = "CLOSED"
	StateOpen     State = "OPEN"
	StateHalfOpen State = "HALF_OPEN"
)

// Config defines the operational parameters of the circuit breaker.
type Config struct {
	// FailureThreshold is the number of consecutive failures required to trip the breaker from CLOSED to OPEN.
	FailureThreshold int
	// Cooldown is the duration the breaker remains in OPEN before transitioning to HALF_OPEN.
	Cooldown time.Duration
	// HalfOpenSuccessThreshold is the number of consecutive successful probe calls needed in HALF_OPEN to close the breaker.
	HalfOpenSuccessThreshold int
	// OnStateChange is an optional callback invoked whenever a state transition occurs.
	OnStateChange func(from, to State)
}

// DefaultConfig provides recommended production defaults.
func DefaultConfig() Config {
	return Config{
		FailureThreshold:         5,
		Cooldown:                 5 * time.Second,
		HalfOpenSuccessThreshold: 3,
	}
}

// Stats holds observability metrics for the circuit breaker.
type Stats struct {
	State                    State         `json:"state"`
	OpenCount                int64         `json:"open_count"`
	TotalOpenDuration        time.Duration `json:"total_open_duration"`
	ConsecutiveFailures      int           `json:"consecutive_failures"`
	ConsecutiveSuccesses     int           `json:"consecutive_successes"`
	CurrentOpenDuration      time.Duration `json:"current_open_duration,omitempty"`
}

// CircuitBreaker implements the 3-state circuit breaker pattern with thread safety and telemetry.
type CircuitBreaker struct {
	mu                       sync.Mutex
	config                   Config
	state                    State
	consecutiveFailures      int
	consecutiveSuccesses     int
	lastStateChange          time.Time
	openedAt                 time.Time
	openCount                int64
	totalOpenDuration        time.Duration
}

// New creates and initializes a CircuitBreaker with the given configuration.
func New(cfg Config) *CircuitBreaker {
	if cfg.FailureThreshold <= 0 {
		cfg.FailureThreshold = 5
	}
	if cfg.Cooldown <= 0 {
		cfg.Cooldown = 5 * time.Second
	}
	if cfg.HalfOpenSuccessThreshold <= 0 {
		cfg.HalfOpenSuccessThreshold = 3
	}

	now := time.Now()
	return &CircuitBreaker{
		config:          cfg,
		state:           StateClosed,
		lastStateChange: now,
	}
}

// Execute wraps an operation with circuit breaker protection.
func (cb *CircuitBreaker) Execute(ctx context.Context, fn func() error) error {
	if err := cb.beforeExecution(); err != nil {
		return err
	}

	// Check context cancellation before invoking the external function
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}

	err := fn()
	cb.afterExecution(err)
	return err
}

// beforeExecution checks the current state and handles automatic transition from OPEN to HALF_OPEN.
func (cb *CircuitBreaker) beforeExecution() error {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	now := time.Now()

	switch cb.state {
	case StateOpen:
		// Check if cooldown period has elapsed
		if now.Sub(cb.lastStateChange) >= cb.config.Cooldown {
			cb.transitionTo(StateHalfOpen, now)
			return nil
		}
		return ErrCircuitOpen

	case StateHalfOpen:
		// Allow probe requests through in HALF_OPEN
		return nil

	case StateClosed:
		// Normal operation: all requests pass through
		return nil
	}

	return nil
}

// afterExecution updates counters and triggers state transitions based on operation outcome.
func (cb *CircuitBreaker) afterExecution(err error) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	now := time.Now()

	if err != nil {
		cb.onFailure(now)
	} else {
		cb.onSuccess(now)
	}
}

// onFailure processes a failed operation attempt.
func (cb *CircuitBreaker) onFailure(now time.Time) {
	switch cb.state {
	case StateClosed:
		cb.consecutiveFailures++
		if cb.consecutiveFailures >= cb.config.FailureThreshold {
			cb.transitionTo(StateOpen, now)
		}

	case StateHalfOpen:
		// Any failure during trial in HALF_OPEN trips the breaker back to OPEN
		cb.transitionTo(StateOpen, now)

	case StateOpen:
		// Already open; cooldown timer remains tracked from lastStateChange
	}
}

// onSuccess processes a successful operation attempt.
func (cb *CircuitBreaker) onSuccess(now time.Time) {
	switch cb.state {
	case StateClosed:
		// Decrement failure count on success to accommodate concurrent interleaving
		if cb.consecutiveFailures > 0 {
			cb.consecutiveFailures--
		}

	case StateHalfOpen:
		cb.consecutiveSuccesses++
		if cb.consecutiveSuccesses >= cb.config.HalfOpenSuccessThreshold {
			cb.transitionTo(StateClosed, now)
		}

	case StateOpen:
		// In OPEN state, no success should normally occur
	}
}

// transitionTo performs the state switch and updates all related timers and metrics.
// Caller MUST hold cb.mu.
func (cb *CircuitBreaker) transitionTo(newState State, now time.Time) {
	oldState := cb.state
	if oldState == newState {
		return
	}

	// Update open duration when exiting OPEN state
	if oldState == StateOpen {
		openDuration := now.Sub(cb.openedAt)
		cb.totalOpenDuration += openDuration
	}

	// Record initial opening
	if newState == StateOpen {
		cb.openedAt = now
		cb.openCount++
	}

	cb.state = newState
	cb.lastStateChange = now
	cb.consecutiveFailures = 0
	cb.consecutiveSuccesses = 0

	if cb.config.OnStateChange != nil {
		// Run callback synchronously or safely under lock
		cb.config.OnStateChange(oldState, newState)
	}
}

// State returns the current operating state of the circuit breaker.
func (cb *CircuitBreaker) State() State {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	// Check if OPEN cooldown has expired to report accurate current state
	if cb.state == StateOpen && time.Since(cb.lastStateChange) >= cb.config.Cooldown {
		cb.transitionTo(StateHalfOpen, time.Now())
	}

	return cb.state
}

// OpenCount returns the total number of times the breaker has transitioned to OPEN.
func (cb *CircuitBreaker) OpenCount() int64 {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.openCount
}

// TotalOpenDuration returns the cumulative duration the breaker has been in OPEN state.
func (cb *CircuitBreaker) TotalOpenDuration() time.Duration {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	total := cb.totalOpenDuration
	if cb.state == StateOpen {
		total += time.Since(cb.openedAt)
	}
	return total
}

// Stats returns a snapshot of breaker performance and state.
func (cb *CircuitBreaker) Stats() Stats {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	total := cb.totalOpenDuration
	var currentOpen time.Duration
	if cb.state == StateOpen {
		currentOpen = time.Since(cb.openedAt)
		total += currentOpen
	}

	return Stats{
		State:                cb.state,
		OpenCount:            cb.openCount,
		TotalOpenDuration:    total,
		ConsecutiveFailures:  cb.consecutiveFailures,
		ConsecutiveSuccesses: cb.consecutiveSuccesses,
		CurrentOpenDuration:  currentOpen,
	}
}

// PrintReport logs formatted circuit breaker metrics.
func (cb *CircuitBreaker) PrintReport() {
	s := cb.Stats()
	fmt.Printf("Circuit Breaker Telemetry (internal/breaker):\n")
	fmt.Printf("  • Current State               : %s\n", s.State)
	fmt.Printf("  • Total Open Events (trips)   : %d\n", s.OpenCount)
	fmt.Printf("  • Cumulative Time in OPEN     : %s\n", s.TotalOpenDuration.Round(time.Millisecond))
	fmt.Printf("  • Failure Threshold           : %d consecutive failures\n", cb.config.FailureThreshold)
	fmt.Printf("  • Cooldown Duration           : %s\n", cb.config.Cooldown)
	fmt.Printf("  • Half-Open Probe Success Req : %d successful calls\n", cb.config.HalfOpenSuccessThreshold)
}
