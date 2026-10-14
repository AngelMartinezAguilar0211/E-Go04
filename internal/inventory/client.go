package inventory

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"time"

	"e-go04/internal/breaker"
)

var (
	// ErrServiceOverloaded is returned when the external inventory service responds with HTTP 503.
	ErrServiceOverloaded = errors.New("inventory service overloaded (HTTP 503)")
	// ErrUnexpectedStatus is returned when an unexpected HTTP status code is received.
	ErrUnexpectedStatus = errors.New("unexpected status from inventory service")
)

type checkRequest struct {
	OrderID string `json:"order_id"`
	Country string `json:"country"`
}

type checkResponse struct {
	Available bool   `json:"available,omitempty"`
	Error     string `json:"error,omitempty"`
}

// ClientTelemetry records execution counts for inventory verification calls.
type ClientTelemetry struct {
	TotalCalls        int64 `json:"total_calls"`
	SuccessCalls      int64 `json:"success_calls"`
	OverloadedCalls   int64 `json:"overloaded_calls"`
	CircuitOpenCalls  int64 `json:"circuit_open_calls"`
	NetworkErrorCalls int64 `json:"network_error_calls"`
}

// Client wraps HTTP communication with the external simulated inventory service.
type Client struct {
	baseURL           string
	httpClient        *http.Client
	cb                *breaker.CircuitBreaker
	totalCalls        int64
	successCalls      int64
	overloadedCalls   int64
	circuitOpenCalls  int64
	networkErrorCalls int64
}

// NewClient initializes an InventoryClient with configured circuit breaker and timeout.
func NewClient(baseURL string, cb *breaker.CircuitBreaker, timeout time.Duration) *Client {
	if timeout <= 0 {
		timeout = 3 * time.Second
	}

	transport := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 100,
		IdleConnTimeout:     90 * time.Second,
		DisableKeepAlives:   false,
	}

	return &Client{
		baseURL: baseURL,
		httpClient: &http.Client{
			Transport: transport,
			Timeout:   timeout,
		},
		cb: cb,
	}
}

// CheckInventory performs the inventory availability check for an order, guarded by the circuit breaker.
func (c *Client) CheckInventory(ctx context.Context, orderID, country string) error {
	atomic.AddInt64(&c.totalCalls, 1)

	// Wrap execution with circuit breaker
	err := c.cb.Execute(ctx, func() error {
		reqBody := checkRequest{
			OrderID: orderID,
			Country: country,
		}
		data, err := json.Marshal(reqBody)
		if err != nil {
			return fmt.Errorf("failed to marshal inventory request: %w", err)
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/check-inventory", bytes.NewReader(data))
		if err != nil {
			return fmt.Errorf("failed to create inventory request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return fmt.Errorf("network error contacting inventory service: %w", err)
		}
		defer resp.Body.Close()

		// Read response body
		body, _ := io.ReadAll(resp.Body)

		switch resp.StatusCode {
		case http.StatusOK:
			var res checkResponse
			if err := json.Unmarshal(body, &res); err == nil && res.Available {
				return nil
			}
			return nil // 200 OK with available=true
		case http.StatusServiceUnavailable:
			return ErrServiceOverloaded
		default:
			return fmt.Errorf("%w: HTTP %d (body: %s)", ErrUnexpectedStatus, resp.StatusCode, string(body))
		}
	})

	if err == nil {
		atomic.AddInt64(&c.successCalls, 1)
		return nil
	}

	if errors.Is(err, breaker.ErrCircuitOpen) {
		atomic.AddInt64(&c.circuitOpenCalls, 1)
	} else if errors.Is(err, ErrServiceOverloaded) {
		atomic.AddInt64(&c.overloadedCalls, 1)
	} else {
		atomic.AddInt64(&c.networkErrorCalls, 1)
	}

	return err
}

// Telemetry returns execution statistics.
func (c *Client) Telemetry() ClientTelemetry {
	return ClientTelemetry{
		TotalCalls:        atomic.LoadInt64(&c.totalCalls),
		SuccessCalls:      atomic.LoadInt64(&c.successCalls),
		OverloadedCalls:   atomic.LoadInt64(&c.overloadedCalls),
		CircuitOpenCalls:  atomic.LoadInt64(&c.circuitOpenCalls),
		NetworkErrorCalls: atomic.LoadInt64(&c.networkErrorCalls),
	}
}

// PrintReport logs formatted metrics for the inventory client calls.
func (c *Client) PrintReport() {
	t := c.Telemetry()
	fmt.Printf("Inventory Service Client Metrics (internal/inventory):\n")
	fmt.Printf("  • Total Invocations          : %d\n", t.TotalCalls)
	fmt.Printf("  • Successful Checks (200 OK) : %d\n", t.SuccessCalls)
	fmt.Printf("  • Overloaded Responses (503) : %d\n", t.OverloadedCalls)
	fmt.Printf("  • Fast-Failed by Breaker     : %d\n", t.CircuitOpenCalls)
	fmt.Printf("  • Network / Other Errors     : %d\n", t.NetworkErrorCalls)
}
