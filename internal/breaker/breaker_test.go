package breaker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var dummyErr = errors.New("simulated external service error")

func TestCircuitBreaker_StartsClosed(t *testing.T) {
	cb := New(DefaultConfig())
	if state := cb.State(); state != StateClosed {
		t.Fatalf("expected initial state %s, got %s", StateClosed, state)
	}

	executed := false
	err := cb.Execute(context.Background(), func() error {
		executed = true
		return nil
	})

	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if !executed {
		t.Fatalf("expected function to be executed")
	}
}

func TestCircuitBreaker_OpensAfterThreshold(t *testing.T) {
	var transitions []string
	cfg := Config{
		FailureThreshold:         3,
		Cooldown:                 100 * time.Millisecond,
		HalfOpenSuccessThreshold: 2,
		OnStateChange: func(from, to State) {
			transitions = append(transitions, string(from)+"->"+string(to))
		},
	}
	cb := New(cfg)

	// Cause 2 failures (threshold is 3)
	for i := 0; i < 2; i++ {
		_ = cb.Execute(context.Background(), func() error { return dummyErr })
		if cb.State() != StateClosed {
			t.Fatalf("expected CLOSED after %d failures, got %s", i+1, cb.State())
		}
	}

	// 3rd failure trips the breaker
	_ = cb.Execute(context.Background(), func() error { return dummyErr })
	if cb.State() != StateOpen {
		t.Fatalf("expected OPEN after 3 failures, got %s", cb.State())
	}
	if cb.OpenCount() != 1 {
		t.Fatalf("expected openCount 1, got %d", cb.OpenCount())
	}
	if len(transitions) != 1 || transitions[0] != "CLOSED->OPEN" {
		t.Fatalf("unexpected transitions: %v", transitions)
	}
}

func TestCircuitBreaker_FastFailsWhenOpen(t *testing.T) {
	cfg := Config{
		FailureThreshold:         1,
		Cooldown:                 500 * time.Millisecond,
		HalfOpenSuccessThreshold: 1,
	}
	cb := New(cfg)

	// Trip to OPEN
	_ = cb.Execute(context.Background(), func() error { return dummyErr })
	if cb.State() != StateOpen {
		t.Fatalf("expected state OPEN, got %s", cb.State())
	}

	// Next call should fast-fail without executing fn
	fnCalled := false
	err := cb.Execute(context.Background(), func() error {
		fnCalled = true
		return nil
	})

	if !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("expected ErrCircuitOpen, got %v", err)
	}
	if fnCalled {
		t.Fatalf("expected protected function NOT to be called when OPEN")
	}
}

func TestCircuitBreaker_TransitionsToHalfOpen(t *testing.T) {
	cfg := Config{
		FailureThreshold:         1,
		Cooldown:                 50 * time.Millisecond,
		HalfOpenSuccessThreshold: 2,
	}
	cb := New(cfg)

	// Trip to OPEN
	_ = cb.Execute(context.Background(), func() error { return dummyErr })
	if cb.State() != StateOpen {
		t.Fatalf("expected state OPEN, got %s", cb.State())
	}

	// Wait for cooldown
	time.Sleep(70 * time.Millisecond)

	// State() should now report HALF_OPEN
	if state := cb.State(); state != StateHalfOpen {
		t.Fatalf("expected state %s after cooldown, got %s", StateHalfOpen, state)
	}
}

func TestCircuitBreaker_ClosesAfterHalfOpenSuccesses(t *testing.T) {
	var transitions []string
	cfg := Config{
		FailureThreshold:         1,
		Cooldown:                 50 * time.Millisecond,
		HalfOpenSuccessThreshold: 2,
		OnStateChange: func(from, to State) {
			transitions = append(transitions, string(from)+"->"+string(to))
		},
	}
	cb := New(cfg)

	// Trip to OPEN
	_ = cb.Execute(context.Background(), func() error { return dummyErr })

	// Wait for cooldown to transition to HALF_OPEN
	time.Sleep(60 * time.Millisecond)

	// 1st probe success
	err := cb.Execute(context.Background(), func() error { return nil })
	if err != nil {
		t.Fatalf("probe 1 failed: %v", err)
	}
	if cb.State() != StateHalfOpen {
		t.Fatalf("expected still HALF_OPEN after 1 success, got %s", cb.State())
	}

	// 2nd probe success closes the breaker
	err = cb.Execute(context.Background(), func() error { return nil })
	if err != nil {
		t.Fatalf("probe 2 failed: %v", err)
	}
	if cb.State() != StateClosed {
		t.Fatalf("expected CLOSED after 2 successes, got %s", cb.State())
	}

	expectedTransitions := []string{"CLOSED->OPEN", "OPEN->HALF_OPEN", "HALF_OPEN->CLOSED"}
	if len(transitions) != len(expectedTransitions) {
		t.Fatalf("expected %d transitions, got %d: %v", len(expectedTransitions), len(transitions), transitions)
	}
	for i, tr := range expectedTransitions {
		if transitions[i] != tr {
			t.Errorf("transition %d: expected %s, got %s", i, tr, transitions[i])
		}
	}
}

func TestCircuitBreaker_ReopensOnHalfOpenFailure(t *testing.T) {
	cfg := Config{
		FailureThreshold:         1,
		Cooldown:                 50 * time.Millisecond,
		HalfOpenSuccessThreshold: 2,
	}
	cb := New(cfg)

	// Trip to OPEN
	_ = cb.Execute(context.Background(), func() error { return dummyErr })
	time.Sleep(60 * time.Millisecond)

	// Fail probe in HALF_OPEN
	err := cb.Execute(context.Background(), func() error { return dummyErr })
	if !errors.Is(err, dummyErr) {
		t.Fatalf("expected dummyErr, got %v", err)
	}

	if cb.State() != StateOpen {
		t.Fatalf("expected breaker to reopen to OPEN, got %s", cb.State())
	}
	if cb.OpenCount() != 2 {
		t.Fatalf("expected openCount 2, got %d", cb.OpenCount())
	}
}

func TestCircuitBreaker_TracksOpenDurationAndCount(t *testing.T) {
	cfg := Config{
		FailureThreshold:         1,
		Cooldown:                 50 * time.Millisecond,
		HalfOpenSuccessThreshold: 1,
	}
	cb := New(cfg)

	_ = cb.Execute(context.Background(), func() error { return dummyErr })
	time.Sleep(70 * time.Millisecond)

	// Probe succeeds and closes breaker
	_ = cb.Execute(context.Background(), func() error { return nil })

	if cb.OpenCount() != 1 {
		t.Fatalf("expected openCount 1, got %d", cb.OpenCount())
	}
	duration := cb.TotalOpenDuration()
	if duration < 50*time.Millisecond {
		t.Fatalf("expected open duration >= 50ms, got %v", duration)
	}
}

func TestCircuitBreaker_ConcurrentAccess(t *testing.T) {
	cfg := Config{
		FailureThreshold:         5,
		Cooldown:                 20 * time.Millisecond,
		HalfOpenSuccessThreshold: 2,
	}
	cb := New(cfg)

	var wg sync.WaitGroup
	workers := 20
	iterations := 100
	var successCount int64
	var failCount int64
	var circuitOpenCount int64

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				err := cb.Execute(context.Background(), func() error {
					// Induce bursts of consecutive failures across workers
					if j >= 20 && j <= 35 {
						return dummyErr
					}
					return nil
				})

				if err == nil {
					atomic.AddInt64(&successCount, 1)
				} else if errors.Is(err, ErrCircuitOpen) {
					atomic.AddInt64(&circuitOpenCount, 1)
				} else {
					atomic.AddInt64(&failCount, 1)
				}
				time.Sleep(1 * time.Millisecond)
			}
		}(i)
	}

	wg.Wait()

	total := successCount + failCount + circuitOpenCount
	if total != int64(workers*iterations) {
		t.Fatalf("expected total calls %d, got %d", workers*iterations, total)
	}
	if cb.OpenCount() == 0 {
		t.Fatalf("expected breaker to have tripped at least once during concurrent stress")
	}
}
