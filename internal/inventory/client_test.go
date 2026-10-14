package inventory

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"e-go04/internal/breaker"
)

func TestClient_SuccessCheck(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/check-inventory" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"available": true}`))
	}))
	defer server.Close()

	cb := breaker.New(breaker.DefaultConfig())
	client := NewClient(server.URL, cb, 1*time.Second)

	err := client.CheckInventory(context.Background(), "ord-123", "MX")
	if err != nil {
		t.Fatalf("expected successful check, got error: %v", err)
	}

	tel := client.Telemetry()
	if tel.TotalCalls != 1 || tel.SuccessCalls != 1 || tel.OverloadedCalls != 0 {
		t.Fatalf("unexpected telemetry: %+v", tel)
	}
}

func TestClient_CircuitBreakerIntegration(t *testing.T) {
	var statusCode int32 = http.StatusServiceUnavailable

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		code := atomic.LoadInt32(&statusCode)
		w.WriteHeader(int(code))
		if code == http.StatusOK {
			_, _ = w.Write([]byte(`{"available": true}`))
		} else {
			_, _ = w.Write([]byte(`{"error": "overloaded"}`))
		}
	}))
	defer server.Close()

	// Breaker trips after 2 failures; 50ms cooldown
	cfg := breaker.Config{
		FailureThreshold:         2,
		Cooldown:                 50 * time.Millisecond,
		HalfOpenSuccessThreshold: 1,
	}
	cb := breaker.New(cfg)
	client := NewClient(server.URL, cb, 1*time.Second)

	// 1st failure (503)
	err := client.CheckInventory(context.Background(), "ord-1", "AR")
	if !errors.Is(err, ErrServiceOverloaded) {
		t.Fatalf("call 1: expected ErrServiceOverloaded, got %v", err)
	}

	// 2nd failure (503) -> trips breaker to OPEN
	err = client.CheckInventory(context.Background(), "ord-2", "AR")
	if !errors.Is(err, ErrServiceOverloaded) {
		t.Fatalf("call 2: expected ErrServiceOverloaded, got %v", err)
	}

	if cb.State() != breaker.StateOpen {
		t.Fatalf("expected breaker to be OPEN, got %s", cb.State())
	}

	// 3rd call immediately fast-fails with ErrCircuitOpen
	err = client.CheckInventory(context.Background(), "ord-3", "AR")
	if !errors.Is(err, breaker.ErrCircuitOpen) {
		t.Fatalf("call 3: expected ErrCircuitOpen, got %v", err)
	}

	// Server recovers
	atomic.StoreInt32(&statusCode, http.StatusOK)

	// Wait for cooldown to expire
	time.Sleep(60 * time.Millisecond)

	// Probe call in HALF_OPEN should succeed and close breaker
	err = client.CheckInventory(context.Background(), "ord-4", "AR")
	if err != nil {
		t.Fatalf("probe call failed: %v", err)
	}

	if cb.State() != breaker.StateClosed {
		t.Fatalf("expected breaker to return to CLOSED, got %s", cb.State())
	}

	tel := client.Telemetry()
	if tel.TotalCalls != 4 || tel.SuccessCalls != 1 || tel.OverloadedCalls != 2 || tel.CircuitOpenCalls != 1 {
		t.Fatalf("unexpected telemetry counts: %+v", tel)
	}
}
