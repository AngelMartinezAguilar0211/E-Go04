package backpressure

import (
	"sync"
	"time"

	"e-go04/internal/domain"
)

// RetryItem represents a deferred event waiting for retry due to service degradation.
type RetryItem struct {
	StreamID   string
	Event      *domain.Event
	Retries    int
	NextRetry  time.Time
	EnqueuedAt time.Time
}

// BufferStats contains telemetry on the retry buffer utilization.
type BufferStats struct {
	CurrentDepth  int   `json:"current_depth"`
	MaxDepth      int64 `json:"max_depth"`
	TotalEnqueued int64 `json:"total_enqueued"`
	TotalDequeued int64 `json:"total_dequeued"`
	Capacity      int   `json:"capacity"`
}

// RetryBuffer is a thread-safe FIFO queue with capacity bounding and peak depth tracking.
type RetryBuffer struct {
	mu            sync.Mutex
	items         []*RetryItem
	capacity      int
	maxDepth      int64
	totalEnqueued int64
	totalDequeued int64
}

// NewRetryBuffer initializes a RetryBuffer with the specified maximum capacity.
// A capacity of 0 or negative indicates an unbounded buffer.
func NewRetryBuffer(capacity int) *RetryBuffer {
	if capacity <= 0 {
		capacity = 100000 // Safe default bounded capacity
	}
	return &RetryBuffer{
		items:    make([]*RetryItem, 0, 1024),
		capacity: capacity,
	}
}

// Push adds an item to the buffer. If the buffer has reached maximum capacity,
// it returns false indicating overflow (which triggers upstream ingestion pause).
func (rb *RetryBuffer) Push(item *RetryItem) bool {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	if len(rb.items) >= rb.capacity {
		return false
	}

	if item.EnqueuedAt.IsZero() {
		item.EnqueuedAt = time.Now()
	}

	rb.items = append(rb.items, item)
	rb.totalEnqueued++

	currentDepth := int64(len(rb.items))
	if currentDepth > rb.maxDepth {
		rb.maxDepth = currentDepth
	}

	return true
}

// PopReady retrieves the oldest item whose NextRetry timestamp is <= now.
// Returns nil if no item is ready.
func (rb *RetryBuffer) PopReady(now time.Time) *RetryItem {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	if len(rb.items) == 0 {
		return nil
	}

	// FIFO inspection: check oldest items first
	for i, item := range rb.items {
		if now.After(item.NextRetry) || now.Equal(item.NextRetry) {
			// Remove element at index i while maintaining order
			rb.items = append(rb.items[:i], rb.items[i+1:]...)
			rb.totalDequeued++
			return item
		}
	}

	return nil
}

// Pop retrieves the head of the buffer unconditionally, or nil if empty.
func (rb *RetryBuffer) Pop() *RetryItem {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	if len(rb.items) == 0 {
		return nil
	}

	item := rb.items[0]
	rb.items = rb.items[1:]
	rb.totalDequeued++
	return item
}

// Len returns the current number of elements held in the buffer.
func (rb *RetryBuffer) Len() int {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	return len(rb.items)
}

// MaxDepth returns the peak buffer occupancy reached during execution.
func (rb *RetryBuffer) MaxDepth() int64 {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	return rb.maxDepth
}

// Stats returns a snapshot of buffer metrics.
func (rb *RetryBuffer) Stats() BufferStats {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	return BufferStats{
		CurrentDepth:  len(rb.items),
		MaxDepth:      rb.maxDepth,
		TotalEnqueued: rb.totalEnqueued,
		TotalDequeued: rb.totalDequeued,
		Capacity:      rb.capacity,
	}
}

// Clear resets the buffer items.
func (rb *RetryBuffer) Clear() {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	rb.items = rb.items[:0]
}
